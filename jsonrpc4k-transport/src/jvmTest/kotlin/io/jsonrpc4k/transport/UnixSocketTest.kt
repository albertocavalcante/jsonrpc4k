@file:OptIn(ExperimentalPathApi::class)

package io.jsonrpc4k.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.channels.ServerSocketChannel
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.deleteRecursively
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private const val LONG_COMPONENT = 200

/** Runs [block] with a short-lived directory; socket paths must stay under the sockaddr_un limit. */
private fun withSocketDir(block: suspend CoroutineScope.(Path) -> Unit) {
    val dir = Files.createTempDirectory("jrpc")
    try {
        runBlocking(Dispatchers.Default) { withTimeout(20.seconds) { this.block(dir) } }
    } finally {
        dir.deleteRecursively()
    }
}

class UnixSocketTest {
    @Test
    fun `echo round trip and the socket file is removed on close`() =
        withSocketDir { dir ->
            val socket = dir.resolve("echo.sock")
            val listener = UnixSocket.bind(socket.toString())
            assertEquals(socket.toString(), listener.path)
            assertEquals(-1, listener.port)
            assertTrue(Files.exists(socket))

            val serverSide = async(Dispatchers.IO) { listener.accept() }
            val client = UnixSocket.connect(socket.toString())
            val server = serverSide.await()
            val payload = ByteArray(1024) { it.toByte() }

            client.send(payload)
            assertContentEquals(payload, server.incoming.first())
            server.send(payload.reversedArray())
            assertContentEquals(payload.reversedArray(), client.incoming.first())

            client.close()
            server.close()
            listener.close()
            listener.close()
            assertFalse(Files.exists(socket))
        }

    @Test
    fun `pending accept fails promptly when the listener closes`() =
        withSocketDir { dir ->
            val listener = UnixSocket.bind(dir.resolve("close.sock").toString())
            val pending = async(Dispatchers.IO) { runCatching { listener.accept() } }
            listener.close()
            assertTrue(pending.await().exceptionOrNull() is TransportException)
        }

    @Test
    fun `a stale socket file is replaced`() =
        withSocketDir { dir ->
            val socket = dir.resolve("stale.sock")
            // The JDK leaves the socket file behind when a server channel closes: that is a stale socket.
            ServerSocketChannel.open(StandardProtocolFamily.UNIX).use {
                it.bind(UnixDomainSocketAddress.of(socket))
            }
            assertTrue(Files.exists(socket))

            val listener = UnixSocket.bind(socket.toString())
            val serverSide = async(Dispatchers.IO) { listener.accept() }
            val client = UnixSocket.connect(socket.toString())
            val server = serverSide.await()
            client.send(byteArrayOf(1, 2, 3))
            assertContentEquals(byteArrayOf(1, 2, 3), server.incoming.first())
            client.close()
            server.close()
            listener.close()
        }

    @Test
    fun `a live socket is not replaced`() =
        withSocketDir { dir ->
            val socket = dir.resolve("live.sock").toString()
            val first = UnixSocket.bind(socket)
            assertFailsWith<TransportException> { UnixSocket.bind(socket) }
            val serverSide = async(Dispatchers.IO) { first.accept() }
            UnixSocket.connect(socket).close()
            serverSide.await().close()
            first.close()
        }

    @Test
    fun `a regular file at the path is left alone`() =
        withSocketDir { dir ->
            val file = dir.resolve("regular.sock")
            Files.writeString(file, "keep")
            assertFailsWith<TransportException> { UnixSocket.bind(file.toString()) }
            assertEquals("keep", Files.readString(file))
        }

    @Test
    fun `a path longer than the socket address limit fails with TransportException`() =
        withSocketDir { dir ->
            val tooLong = dir.resolve("a".repeat(LONG_COMPONENT)).toString()
            assertFailsWith<TransportException> { UnixSocket.bind(tooLong) }
            assertFailsWith<TransportException> { UnixSocket.connect(tooLong) }
        }

    @Test
    fun `connecting to a missing socket fails with TransportException`() =
        withSocketDir { dir ->
            assertFailsWith<TransportException> { UnixSocket.connect(dir.resolve("missing.sock").toString()) }
        }

    @Test
    fun `named pipe server is unsupported on the JVM`() =
        withSocketDir {
            assertFailsWith<UnsupportedTransportException> { NamedPipe.bind("jsonrpc") }
        }

    @Test
    fun `Node IPC is unsupported on the JVM`() {
        assertFailsWith<UnsupportedTransportException> { NodeIpc.connect() }
    }

    @Test
    fun `connect by launch spec reaches a Unix socket`() =
        withSocketDir { dir ->
            val socket = dir.resolve("spec.sock").toString()
            val listener = UnixSocket.bind(socket)
            val serverSide = async(Dispatchers.IO) { listener.accept() }
            val client = TransportFactory.connect(LspTransportSpec.Pipe(socket))
            val server = serverSide.await()
            client.send(byteArrayOf(42))
            assertContentEquals(byteArrayOf(42), server.incoming.first())
            client.close()
            server.close()
            listener.close()
        }

    @Test
    fun `connect by launch spec reaches a TCP listener`() =
        withSocketDir {
            val listener = TcpListener.bind("127.0.0.1", 0)
            val serverSide = async(Dispatchers.IO) { listener.accept() }
            val client = TransportFactory.connect(LspTransportSpec.Socket(listener.port))
            val server = serverSide.await()
            client.send(byteArrayOf(7))
            assertContentEquals(byteArrayOf(7), server.incoming.first())
            client.close()
            server.close()
            listener.close()
        }
}

class UnixSocketContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> {
        val dir = Files.createTempDirectory("jrpc")
        val socket = dir.resolve("pair.sock").toString()
        val listener = UnixSocket.bind(socket)
        try {
            return coroutineScope {
                val accepted = async(Dispatchers.IO) { listener.accept() }
                val client = UnixSocket.connect(socket)
                client to accepted.await()
            }
        } finally {
            listener.close()
            Files.deleteIfExists(dir)
        }
    }
}
