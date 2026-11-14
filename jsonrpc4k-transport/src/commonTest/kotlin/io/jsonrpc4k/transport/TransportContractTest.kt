package io.jsonrpc4k.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.TestResult
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val ROUND_TRIP_BYTES = 1024 * 1024
private const val ROUND_TRIP_CHUNK = 4096
private const val ORDERING_MESSAGES = 500
private const val BYTE_RANGE = 256
private val CASE_TIMEOUT = 30.seconds

/**
 * Reusable behavioural contract for [Transport] implementations. Subclass it, supply connected
 * ends in [createPair], and every platform that runs commonTest executes the same checks.
 */
abstract class TransportContractTest {
    /** Two connected ends: bytes sent on `first` arrive on `second` and vice versa. */
    abstract suspend fun createPair(): Pair<Transport, Transport>

    /** False on a host where the transport cannot exist; every case then passes without running. */
    protected open fun isSupportedHere(): Boolean = true

    @Test
    fun `bytes flow in both directions`() =
        contract { a, b ->
            a.send(byteArrayOf(1, 2, 3))
            b.send(byteArrayOf(4, 5))
            val atB = async { collectBytes(b, expected = 3) }
            val atA = async { collectBytes(a, expected = 2) }
            assertContentEquals(byteArrayOf(1, 2, 3), atB.await())
            assertContentEquals(byteArrayOf(4, 5), atA.await())
        }

    @Test
    fun `peer close completes incoming normally with all prior bytes`() =
        contract { a, b ->
            a.send(byteArrayOf(9, 8, 7))
            a.close()
            val received = b.incoming.toList().fold(ByteArray(0)) { acc, chunk -> acc + chunk }
            assertContentEquals(byteArrayOf(9, 8, 7), received)
            assertFalse(b.isConnected)
        }

    @Test
    fun `close is idempotent`() =
        contract { a, b ->
            assertTrue(a.isConnected)
            a.close()
            a.close()
            assertFalse(a.isConnected)
            b.close()
            b.close()
            assertFalse(b.isConnected)
        }

    @Test
    fun `close ends the pending incoming of the closing side without error`() =
        contract { a, _ ->
            val collected = async { a.incoming.toList() }
            yield()
            a.close()
            assertTrue(collected.await().isEmpty())
        }

    @Test
    fun `close from a cancelled coroutine still closes`() =
        contract { a, b ->
            val job =
                launch(start = CoroutineStart.UNDISPATCHED) {
                    try {
                        awaitCancellation()
                    } finally {
                        a.close()
                    }
                }
            job.cancelAndJoin()
            assertFalse(a.isConnected)
            assertTrue(b.incoming.toList().isEmpty())
        }

    @Test
    fun `send after close throws TransportException`() =
        contract { a, b ->
            a.close()
            b.close()
            assertFailsWith<TransportException> { a.send(byteArrayOf(1)) }
            assertFailsWith<TransportException> { b.send(byteArrayOf(1)) }
        }

    @Test
    fun `one MiB of binary data round trips unchanged`() =
        contract { a, b ->
            val payload = binaryPayload(ROUND_TRIP_BYTES)
            val received = async { collectBytes(b, expected = payload.size) }
            val sender =
                launch {
                    var offset = 0
                    while (offset < payload.size) {
                        val end = minOf(offset + ROUND_TRIP_CHUNK, payload.size)
                        a.send(payload.copyOfRange(offset, end))
                        offset = end
                    }
                }
            sender.join()
            assertContentEquals(payload, received.await())
        }

    @Test
    fun `chunks arrive in send order`() =
        contract { a, b ->
            val received = async { collectBytes(b, expected = ORDERING_MESSAGES * 2) }
            for (i in 0 until ORDERING_MESSAGES) {
                a.send(byteArrayOf((i / BYTE_RANGE).toByte(), (i % BYTE_RANGE).toByte()))
            }
            val bytes = received.await()
            for (i in 0 until ORDERING_MESSAGES) {
                val value = (bytes[i * 2].toInt() and 0xFF) * BYTE_RANGE + (bytes[i * 2 + 1].toInt() and 0xFF)
                assertEquals(i, value)
            }
        }

    // runTest's timeout runs on the real clock, unlike a withTimeout inside runTest, which virtual time would expire
    // as soon as the test dispatcher idles. On timeout runTest cancels the case, and the cleanup below closes both
    // ends from that cancelled coroutine, which the contract allows; closing ends any pending read, so a stuck case
    // fails by name instead of hanging the run.
    private fun contract(body: suspend CoroutineScope.(Transport, Transport) -> Unit): TestResult =
        runTest(timeout = CASE_TIMEOUT) {
            if (!isSupportedHere()) return@runTest
            val (a, b) = createPair()
            try {
                body(this, a, b)
            } finally {
                // A close that suspends in a cancelled coroutine throws once it is done, so b is closed regardless.
                try {
                    a.close()
                } finally {
                    b.close()
                }
            }
        }
}

/**
 * Collect from [transport] until [expected] bytes arrived. The collector is cancelled once it has
 * enough, so the transport stays open for further use.
 */
private suspend fun collectBytes(
    transport: Transport,
    expected: Int,
): ByteArray {
    val out = ByteArray(expected)
    var filled = 0
    transport.incoming.first { chunk ->
        chunk.copyInto(out, filled)
        filled += chunk.size
        filled >= expected
    }
    return out
}

/** Binary payload cycling through 0x00-0xFF with a CRLF pair every 1000 bytes. */
private fun binaryPayload(size: Int): ByteArray {
    val bytes = ByteArray(size) { (it % BYTE_RANGE).toByte() }
    var i = 0
    while (i + 1 < size) {
        bytes[i] = '\r'.code.toByte()
        bytes[i + 1] = '\n'.code.toByte()
        i += 1000
    }
    return bytes
}
