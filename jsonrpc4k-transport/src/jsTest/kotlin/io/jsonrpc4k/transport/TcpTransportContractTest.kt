package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

/** Runs the shared contract over Node TCP loopback sockets. */
class TcpTransportContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> {
        val listener = TcpListener.bind("127.0.0.1", 0)
        try {
            val client = TcpSocket.connect("127.0.0.1", listener.port)
            val server = listener.accept()
            return client to server
        } finally {
            listener.close()
        }
    }
}

// runTest skips delays in virtual time; real sockets need the real clock, hence Dispatchers.Default.
class NodeTcpListenerTest {
    @Test
    fun `bind to port 0 reports the chosen port and round trips bytes`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    assertTrue(listener.port > 0)
                    val client = TcpSocket.connect("127.0.0.1", listener.port)
                    val server = listener.accept()
                    val received = async { server.incoming.first() }
                    client.send(byteArrayOf(1, 2, 3))
                    assertContentEquals(byteArrayOf(1, 2, 3), received.await())
                    client.close()
                    server.close()
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `closing the listener fails a pending accept`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                val pending = async { runCatching { listener.accept() } }
                // Let the accept suspend before the listener goes away.
                yield()
                listener.close()
                val failure = pending.await().exceptionOrNull()
                assertTrue(failure is TransportException, "accept failed with $failure")
                assertFailsWith<TransportException> { listener.accept() }
                listener.close()
            }
        }

    @Test
    fun `accepted transports survive listener close`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                val client = TcpSocket.connect("127.0.0.1", listener.port)
                val server = listener.accept()
                listener.close()
                val received = async { server.incoming.first() }
                client.send(byteArrayOf(4, 5))
                assertContentEquals(byteArrayOf(4, 5), received.await())
                client.close()
                server.close()
            }
        }

    @Test
    fun `binding a port in use fails with TransportException`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val first = TcpListener.bind("127.0.0.1", 0)
                try {
                    assertFailsWith<TransportException> { TcpListener.bind("127.0.0.1", first.port) }
                } finally {
                    first.close()
                }
            }
        }
}
