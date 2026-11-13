package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private const val PENDING_MILLIS = 300L

// Real sockets need the real clock, so these run on Dispatchers.Default instead of runTest.
class TcpListenerTest {
    @Test
    fun `loopback smoke test round trips bytes`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    assertTrue(listener.port > 0)
                    val client = TcpSocket.connect("127.0.0.1", listener.port)
                    val server = listener.accept()
                    try {
                        val received = async { server.incoming.first() }
                        client.send(byteArrayOf(1, 2, 3))
                        assertContentEquals(byteArrayOf(1, 2, 3), received.await())
                    } finally {
                        client.close()
                        server.close()
                    }
                } finally {
                    listener.close()
                }
            }
        }

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

    @Test
    fun `binding a port in use fails with TransportException`(): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val first = TcpListener.bind("127.0.0.1", 0)
                try {
                    assertFailsWith<TransportException> { TcpListener.bind("127.0.0.1", first.port) }
                } finally {
                    first.close()
                }
            }
        }
}
