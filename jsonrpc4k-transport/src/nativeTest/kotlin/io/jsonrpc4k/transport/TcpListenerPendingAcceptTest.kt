package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private val PROMPT = 5.seconds
private const val PENDING_MILLIS = 300L

// Both native listeners wait in poll slices, so close and cancellation end a pending accept within a slice.
class TcpListenerPendingAcceptTest {
    @Test
    fun `closing the listener fails a pending accept`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                val pending = async { runCatching { listener.accept() } }
                delay(PENDING_MILLIS)
                listener.close()
                val failure = withTimeoutOrNull(PROMPT) { pending.await() }?.exceptionOrNull()
                assertTrue(failure is TransportException, "accept failed with $failure")
                assertFailsWith<TransportException> { listener.accept() }
                listener.close()
            }
        }

    @Test
    fun `cancelling a pending accept ends it and keeps the listener usable`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val pending = launch { listener.accept() }
                    delay(PENDING_MILLIS)
                    val cancelled = withTimeoutOrNull(PROMPT) { pending.cancelAndJoin() } != null
                    assertTrue(cancelled, "the cancelled accept did not end within $PROMPT")
                    val client = TcpSocket.connect("127.0.0.1", listener.port)
                    val server = listener.accept()
                    client.close()
                    server.close()
                } finally {
                    listener.close()
                }
            }
        }
}
