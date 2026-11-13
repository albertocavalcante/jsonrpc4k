package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.random.Random
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private const val PENDING_MILLIS = 300L

// A short fixed directory keeps the path inside the sockaddr_un limit on every host.
private fun tempSocketPath(): String = "/tmp/jrpc4k-${Random.nextLong().toULong()}.sock"

/** Runs the shared contract over Unix domain sockets on Linux and Apple native. */
class UnixSocketContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> {
        val path = tempSocketPath()
        val listener = UnixSocket.bind(path)
        try {
            val client = UnixSocket.connect(path)
            val server = listener.accept()
            return client to server
        } finally {
            listener.close()
        }
    }
}

class UnixListenerTest {
    @Test
    fun `closing a unix listener fails a pending accept`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val listener = UnixSocket.bind(tempSocketPath())
                val pending = async { runCatching { listener.accept() } }
                delay(PENDING_MILLIS)
                listener.close()
                val failure = pending.await().exceptionOrNull()
                assertTrue(failure is TransportException, "accept failed with $failure")
                assertFailsWith<TransportException> { listener.accept() }
            }
        }
}
