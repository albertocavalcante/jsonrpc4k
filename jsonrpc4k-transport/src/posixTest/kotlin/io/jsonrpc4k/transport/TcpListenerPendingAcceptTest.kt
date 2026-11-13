package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private const val PENDING_MILLIS = 300L

// POSIX only: the POSIX listener polls, so close wakes a pending accept. A Windows blocking accept may not wake.
class TcpListenerPendingAcceptTest {
    @Test
    fun `closing the listener fails a pending accept`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                val pending = async { runCatching { listener.accept() } }
                delay(PENDING_MILLIS)
                listener.close()
                val failure = pending.await().exceptionOrNull()
                assertTrue(failure is TransportException, "accept failed with $failure")
                assertFailsWith<TransportException> { listener.accept() }
                listener.close()
            }
        }
}
