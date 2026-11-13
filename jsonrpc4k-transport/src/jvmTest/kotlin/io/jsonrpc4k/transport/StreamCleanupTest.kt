package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.PrintStream
import java.net.InetAddress
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

class StreamCleanupTest {
    @Test
    fun `EOF does not prevent explicit stream disposal`() =
        runTest {
            val input = CloseInput()
            val output = CloseOutput()
            val transport = StreamTransport(input, output)
            assertTrue(transport.incoming.toList().isEmpty())
            assertFalse(transport.isConnected)
            transport.close()
            transport.close()
            assertEquals(1, input.closes.get())
            assertEquals(1, output.closes.get())
        }

    @Test
    fun `a canceled caller still disposes both streams`() =
        runTest {
            val input = CloseInput()
            val output = CloseOutput()
            val transport = StreamTransport(input, output)
            val closer =
                launch {
                    coroutineContext[Job]!!.cancel()
                    transport.close()
                }
            closer.join()
            assertEquals(1, input.closes.get())
            assertEquals(1, output.closes.get())
            transport.close()
            assertEquals(1, input.closes.get())
            assertEquals(1, output.closes.get())
        }

    @Test
    fun `failed close reports the cause and retries only the failed resource`() =
        runTest {
            val failure = IOException("input close failed")
            val input = CloseInput { attempt -> if (attempt == 1) throw failure }
            val output = CloseOutput()
            val transport = StreamTransport(input, output)
            val reported = assertFailsWith<TransportException> { transport.close() }
            assertTrue(reported.hasCause(failure))
            assertFalse(transport.isConnected)
            assertEquals(1, output.closes.get())
            transport.close()
            transport.close()
            assertEquals(2, input.closes.get())
            assertEquals(1, output.closes.get())
        }

    @Test
    fun `both close failures remain observable`() =
        runTest {
            val first = IOException("input close failed")
            val second = IOException("output close failed")
            val input = CloseInput { throw first }
            val output = CloseOutput { throw second }
            val reported = assertFailsWith<TransportException> { StreamTransport(input, output).close() }
            assertTrue(reported.hasCause(first))
            assertTrue(first.suppressed.any { it === second })
            assertEquals(1, input.closes.get())
            assertEquals(1, output.closes.get())
        }

    @Test
    fun `fatal close failure is rethrown after attempting both resources`() =
        runTest {
            val ordinary = IOException("input close failed")
            val fatal = AssertionError("fatal output close")
            val input = CloseInput { throw ordinary }
            val output = CloseOutput { throw fatal }
            val reported = assertFailsWith<AssertionError> { StreamTransport(input, output).close() }
            assertTrue(reported.hasCause(fatal))
            assertTrue(fatal.suppressed.any { it === ordinary })
            assertEquals(1, input.closes.get())
            assertEquals(1, output.closes.get())
        }

    @Test
    fun `concurrent close waits for cleanup and disposes each stream once`() =
        runBlocking {
            val entered = CompletableDeferred<Unit>()
            val release = CountDownLatch(1)
            val input =
                CloseInput {
                    entered.complete(Unit)
                    check(release.await(10, TimeUnit.SECONDS)) { "Close gate was not released" }
                }
            val output = CloseOutput()
            val transport = StreamTransport(input, output)
            try {
                val first = async { transport.close() }
                withTimeout(5.seconds) { entered.await() }
                val second = async(start = CoroutineStart.UNDISPATCHED) { transport.close() }
                assertFalse(second.isCompleted)
                release.countDown()
                withTimeout(5.seconds) {
                    first.await()
                    second.await()
                }
                assertEquals(1, input.closes.get())
                assertEquals(1, output.closes.get())
            } finally {
                release.countDown()
            }
        }

    @Test
    fun `socket disposal after EOF closes the actual connection`() =
        runBlocking {
            ServerSocket(0, 1, InetAddress.getByName("127.0.0.1")).use { listener ->
                val accepted = async(Dispatchers.IO) { listener.accept() }
                val transport = SocketTransport.connect("127.0.0.1", listener.localPort)
                try {
                    withTimeout(5.seconds) { accepted.await() }.use { peer ->
                        peer.soTimeout = 5_000
                        peer.shutdownOutput()
                        assertTrue(withTimeout(5.seconds) { transport.incoming.toList() }.isEmpty())
                        transport.close()
                        assertFalse(transport.isConnected)
                        assertEquals(-1, withContext(Dispatchers.IO) { peer.getInputStream().read() })
                    }
                } finally {
                    transport.close()
                }
            }
        }

    @Test
    fun `concurrent sends never interleave payloads`() =
        runBlocking {
            val output = SlowOutput()
            val transport = StreamTransport(CloseInput(), output)
            val frames = List(8 * 16) { id -> "$id;".repeat(16) }
            coroutineScope {
                frames.chunked(16).forEach { batch ->
                    launch(Dispatchers.IO) { batch.forEach { transport.send("$it\n".encodeToByteArray()) } }
                }
            }
            transport.close()
            assertEquals(1, output.maxWriters.get())
            val written = output.text().lines().dropLast(1)
            assertEquals(frames.sorted(), written.sorted())
        }

    @Test
    fun `close returns while a blocked reader holds the input lock`() =
        runBlocking {
            val input = LockedInput()
            val output = CloseOutput()
            val transport = StreamTransport(input, output, closeTimeout = 200.milliseconds)
            val reader = async(Dispatchers.IO) { transport.incoming.toList() }
            try {
                withTimeout(5.seconds) { input.reading.await() }
                val elapsed = measureTime { transport.close() }
                assertTrue(elapsed < 5.seconds, "close took $elapsed")
                assertFalse(transport.isConnected)
                assertEquals(1, output.closes.get())
                assertEquals(0, input.closes.get())
            } finally {
                input.release()
            }
            // The chunk that the blocked read returns after close is dropped, and the abandoned close then finishes.
            assertTrue(withTimeout(5.seconds) { reader.await() }.isEmpty())
            withTimeout(5.seconds) { input.closed.await() }
            transport.close()
            assertEquals(1, input.closes.get())
            assertEquals(1, output.closes.get())
        }

    @Test
    fun `stdio close leaves the process streams open`() =
        runBlocking {
            val originalIn = System.`in`
            val originalOut = System.out
            val input = CloseInput()
            val output = RecordingOutput()
            System.setIn(input)
            System.setOut(PrintStream(output, false))
            try {
                val transport = StdioTransport()
                transport.send("frame".encodeToByteArray())
                transport.close()
                transport.close()
                assertFalse(transport.isConnected)
                assertFailsWith<TransportException> { transport.send(byteArrayOf(1)) }
                assertEquals("frame", output.toByteArray().decodeToString())
                assertEquals(0, input.closes.get())
                assertEquals(0, output.closes.get())
            } finally {
                System.setIn(originalIn)
                System.setOut(originalOut)
            }
        }

    @Test
    fun `stdio send fails when standard output records an error`() =
        runBlocking {
            val originalOut = System.out
            System.setOut(PrintStream(FailingOutput(), false))
            try {
                val transport = StdioTransport()
                assertFailsWith<TransportException> { transport.send("frame".encodeToByteArray()) }
                transport.close()
            } finally {
                System.setOut(originalOut)
            }
        }

    private fun Throwable.hasCause(original: Throwable): Boolean = generateSequence(this) { it.cause }.take(8).any { it === original }
}

private class CloseInput(
    private val onClose: (Int) -> Unit = {},
) : InputStream() {
    val closes = AtomicInteger()

    override fun read(): Int = -1

    override fun close() = onClose(closes.incrementAndGet())
}

private class CloseOutput(
    private val onClose: (Int) -> Unit = {},
) : OutputStream() {
    val closes = AtomicInteger()

    override fun write(b: Int) = Unit

    override fun close() = onClose(closes.incrementAndGet())
}

/** Writes one byte at a time and records the largest number of overlapping writers. */
private class SlowOutput : OutputStream() {
    private val writers = AtomicInteger()
    private val bytes = ByteArrayOutputStream()
    val maxWriters = AtomicInteger()

    override fun write(b: Int) = write(byteArrayOf(b.toByte()), 0, 1)

    override fun write(
        b: ByteArray,
        off: Int,
        len: Int,
    ) {
        val active = writers.incrementAndGet()
        maxWriters.accumulateAndGet(active) { current, candidate -> maxOf(current, candidate) }
        try {
            for (index in off until off + len) {
                synchronized(bytes) { bytes.write(b[index].toInt()) }
                Thread.yield()
            }
        } finally {
            writers.decrementAndGet()
        }
    }

    fun text(): String = synchronized(bytes) { bytes.toByteArray().decodeToString() }
}

/** Stand-in for a process pipe or console: closing needs the lock that a blocked read holds. */
private class LockedInput : InputStream() {
    private val lock = Any()
    private val gate = CountDownLatch(1)
    val reading = CompletableDeferred<Unit>()
    val closed = CompletableDeferred<Unit>()
    val closes = AtomicInteger()

    override fun read(): Int = error("single-byte reads are not used")

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int =
        synchronized(lock) {
            reading.complete(Unit)
            check(gate.await(10, TimeUnit.SECONDS)) { "Blocked read was not released" }
            b[off] = 1
            1
        }

    override fun close() {
        synchronized(lock) {
            closes.incrementAndGet()
            closed.complete(Unit)
        }
    }

    fun release() = gate.countDown()
}

private class RecordingOutput : ByteArrayOutputStream() {
    val closes = AtomicInteger()

    override fun close() {
        closes.incrementAndGet()
    }
}

private class FailingOutput : OutputStream() {
    override fun write(b: Int): Unit = throw IOException("standard output is gone")
}
