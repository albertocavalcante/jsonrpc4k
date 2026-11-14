package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.concurrent.AtomicInt
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds

class OpenOnIoTest {
    // An accept or connect that returns just after its caller was cancelled must not leak the socket it opened.
    @Test
    fun `a transport opened as the caller is cancelled is closed`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val entered = CompletableDeferred<Unit>()
                val release = AtomicInt(0)
                val transport = RecordingTransport()
                val opening =
                    launch {
                        openOnIo {
                            entered.complete(Unit)
                            // Stands in for a blocking accept that completes after the cancellation.
                            var spins = 0L
                            while (release.value == 0) spins++
                            transport
                        }
                    }
                entered.await()
                opening.cancel()
                release.value = 1
                opening.join()
                assertEquals(1, transport.closes.value, "the discarded transport was not closed")
            }
        }

    @Test
    fun `a transport opened for an active caller is returned open`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val transport = RecordingTransport()
                assertSame(transport, openOnIo { transport })
                assertEquals(0, transport.closes.value)
            }
        }

    @Test
    fun `a failing open propagates its exception`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                assertFailsWith<TransportException> {
                    openOnIo<RecordingTransport> { throw TransportException("refused") }
                }
            }
        }
}

private class RecordingTransport : Transport {
    val closes = AtomicInt(0)

    override val incoming: Flow<ByteArray> = emptyFlow()

    override suspend fun send(data: ByteArray) = Unit

    override suspend fun close() {
        closes.incrementAndGet()
    }

    override val isConnected: Boolean
        get() = closes.value == 0
}
