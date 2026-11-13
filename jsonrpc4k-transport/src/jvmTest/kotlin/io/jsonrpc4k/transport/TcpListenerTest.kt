package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.ByteArrayOutputStream
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class TcpListenerTest {
    @Test
    fun `bind to port 0 reports the chosen port and round trips bytes`() =
        runBlocking(Dispatchers.Default) {
            withTimeout(10.seconds) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    assertTrue(listener.port > 0)
                    val client = SocketTransport.connect("127.0.0.1", listener.port)
                    val server = listener.accept()
                    val echo = launch(Dispatchers.IO) { server.incoming.collect { server.send(it) } }
                    val replies = Channel<ByteArray>(Channel.UNLIMITED)
                    val reader = launch(Dispatchers.IO) { client.incoming.collect { replies.send(it) } }

                    val payload = ByteArray(BINARY_SIZE) { it.toByte() }
                    client.send(payload)
                    assertContentEquals(payload, replies.receiveAtLeast(payload.size))

                    client.close()
                    server.close()
                    echo.join()
                    reader.join()
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `closing the listener fails a pending accept`() =
        runBlocking(Dispatchers.Default) {
            withTimeout(10.seconds) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                val pending = async { runCatching { listener.accept() } }
                delay(300)
                listener.close()
                val failure = pending.await().exceptionOrNull()
                assertTrue(failure is TransportException, "accept failed with $failure")
                assertFailsWith<TransportException> { listener.accept() }
                listener.close()
            }
        }

    @Test
    fun `accepted transports survive listener close`() =
        runBlocking(Dispatchers.Default) {
            withTimeout(10.seconds) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                val client = SocketTransport.connect("127.0.0.1", listener.port)
                val server = listener.accept()
                listener.close()

                val received = CompletableDeferred<ByteArray>()
                val reader = launch(Dispatchers.IO) { server.incoming.collect { received.complete(it) } }
                client.send(byteArrayOf(1, 2, 3))
                assertContentEquals(byteArrayOf(1, 2, 3), received.await())

                client.close()
                server.close()
                reader.join()
            }
        }

    @Test
    fun `cancelling a pending accept returns without closing the listener`() =
        runBlocking(Dispatchers.Default) {
            withTimeout(10.seconds) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val pending = async { listener.accept() }
                    delay(300)
                    pending.cancelAndJoin()

                    val client = SocketTransport.connect("127.0.0.1", listener.port)
                    val server = listener.accept()
                    client.close()
                    server.close()
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `binding a port in use fails with TransportException`() =
        runBlocking(Dispatchers.Default) {
            withTimeout(10.seconds) {
                val first = TcpListener.bind("127.0.0.1", 0)
                try {
                    assertFailsWith<TransportException> { TcpListener.bind("127.0.0.1", first.port) }
                } finally {
                    first.close()
                }
                Unit
            }
        }

    private suspend fun Channel<ByteArray>.receiveAtLeast(size: Int): ByteArray {
        val out = ByteArrayOutputStream()
        while (out.size() < size) {
            out.write(receive())
        }
        return out.toByteArray()
    }

    private companion object {
        const val BINARY_SIZE = 64 * 1024
    }
}
