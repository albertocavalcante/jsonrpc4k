package io.jsonrpc4k.transport

import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

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
            val closer = launch {
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

    private fun Throwable.hasCause(original: Throwable): Boolean =
        generateSequence(this) { it.cause }.take(8).any { it === original }
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
