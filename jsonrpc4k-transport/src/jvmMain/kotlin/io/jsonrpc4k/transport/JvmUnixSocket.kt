package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.net.ConnectException
import java.net.SocketTimeoutException
import java.net.StandardProtocolFamily
import java.net.UnixDomainSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.ClosedChannelException
import java.nio.channels.ClosedSelectorException
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.ServerSocketChannel
import java.nio.channels.SocketChannel
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

// A blocked read or accept wakes up this often so coroutine cancellation and close are noticed.
private const val READ_POLL_MILLIS = 100L
private const val ACCEPT_POLL_MILLIS = 250L

internal actual suspend fun connectUnixSocket(path: String): Transport =
    withContext(Dispatchers.IO) {
        translateUnixFailures("connect to Unix socket $path") {
            val channel = SocketChannel.open(StandardProtocolFamily.UNIX)
            try {
                channel.connect(UnixDomainSocketAddress.of(path))
                UnixSocketTransport.wrap(channel)
            } catch (e: Throwable) {
                channel.close()
                throw e
            }
        }
    }

internal actual suspend fun bindUnixSocket(path: String): LocalListener =
    withContext(Dispatchers.IO) {
        translateUnixFailures("listen on Unix socket $path") { JvmUnixListener.bind(path) }
    }

/** Maps JDK failures to the transport exceptions; [TransportException]s pass through unchanged. */
@Suppress("ThrowsCount")
private inline fun <T> translateUnixFailures(
    action: String,
    block: () -> T,
): T =
    try {
        block()
    } catch (e: TransportException) {
        throw e
    } catch (e: UnsupportedOperationException) {
        throw UnsupportedTransportException("Unix domain sockets are not available on this JVM", e)
    } catch (e: Exception) {
        throw TransportException("Failed to $action", e)
    }

/**
 * Non-blocking view of a [SocketChannel] as streams. Reads and writes wait in a [Selector] with a
 * timeout, so [StreamTransport] can notice cancellation between attempts (a read that finds nothing
 * throws [SocketTimeoutException]) and [close] never has to interrupt a blocked native call.
 */
internal class PolledChannel(
    private val channel: SocketChannel,
) {
    private val readSelector: Selector
    private val writeSelector: Selector

    init {
        channel.configureBlocking(false)
        readSelector = Selector.open()
        writeSelector = Selector.open()
        channel.register(readSelector, SelectionKey.OP_READ)
        channel.register(writeSelector, SelectionKey.OP_WRITE)
    }

    val input: InputStream =
        object : InputStream() {
            override fun read(): Int {
                val single = ByteArray(1)
                var count = 0
                while (count == 0) count = read(single, 0, 1)
                return if (count < 0) -1 else single[0].toInt() and BYTE_MASK
            }

            override fun read(
                b: ByteArray,
                off: Int,
                len: Int,
            ): Int {
                if (len == 0) return 0
                val buffer = ByteBuffer.wrap(b, off, len)
                var count = channel.read(buffer)
                if (count == 0 && await(readSelector, READ_POLL_MILLIS) > 0) count = channel.read(buffer)
                if (count == 0) throw SocketTimeoutException()
                return count
            }

            override fun close() = this@PolledChannel.close()
        }

    val output: OutputStream =
        object : OutputStream() {
            override fun write(b: Int) = write(byteArrayOf(b.toByte()), 0, 1)

            override fun write(
                b: ByteArray,
                off: Int,
                len: Int,
            ) {
                val buffer = ByteBuffer.wrap(b, off, len)
                while (buffer.hasRemaining()) {
                    channel.write(buffer)
                    if (buffer.hasRemaining()) await(writeSelector, READ_POLL_MILLIS)
                }
            }

            // Closing the output half-closes, so the peer sees EOF while the input is still drained.
            override fun close() {
                try {
                    channel.shutdownOutput()
                } catch (ignored: ClosedChannelException) {
                    // Already closed: nothing left to signal.
                }
            }
        }

    private fun await(
        selector: Selector,
        timeoutMillis: Long,
    ): Int =
        try {
            selector.select(timeoutMillis).also { selector.selectedKeys().clear() }
        } catch (ignored: ClosedSelectorException) {
            throw ClosedChannelException()
        }

    private fun close() {
        try {
            channel.close()
        } finally {
            readSelector.close()
            writeSelector.close()
        }
    }

    private companion object {
        const val BYTE_MASK = 0xFF
    }
}

/** Transport over a connected Unix domain [SocketChannel], owned by the transport. */
internal class UnixSocketTransport private constructor(
    private val channel: SocketChannel,
    private val stream: StreamTransport,
) : Transport by stream {
    override val isConnected: Boolean
        get() = channel.isOpen && stream.isConnected

    companion object {
        fun wrap(channel: SocketChannel): UnixSocketTransport {
            val polled = PolledChannel(channel)
            return UnixSocketTransport(channel, StreamTransport(polled.input, polled.output))
        }
    }
}

/**
 * Unix domain socket listener. It owns the server channel and the socket file, which it deletes on
 * [close]; the JDK leaves the file behind.
 */
internal class JvmUnixListener private constructor(
    private val server: ServerSocketChannel,
    private val selector: Selector,
    private val socketFile: Path,
    override val path: String,
) : LocalListener {
    private val closed = AtomicBoolean(false)

    override val port: Int
        get() = -1

    override suspend fun accept(): Transport =
        withContext(Dispatchers.IO) {
            acceptBlocking(coroutineContext)
        }

    private fun acceptBlocking(context: CoroutineContext): Transport {
        while (true) {
            if (!server.isOpen) throw TransportException("Listener is closed")
            try {
                val channel = server.accept()
                if (channel != null) return UnixSocketTransport.wrap(channel)
                selector.select(ACCEPT_POLL_MILLIS)
                selector.selectedKeys().clear()
                context.ensureActive()
            } catch (e: ClosedSelectorException) {
                throw TransportException("Listener is closed", e)
            } catch (e: IOException) {
                val reason = if (server.isOpen) "Accept failed" else "Listener is closed"
                throw TransportException(reason, e)
            }
        }
    }

    override suspend fun close() {
        if (!closed.compareAndSet(false, true)) return
        withContext(NonCancellable + Dispatchers.IO) {
            try {
                server.close()
                selector.close()
            } catch (e: IOException) {
                throw TransportException("Failed to close listener", e)
            } finally {
                Files.deleteIfExists(socketFile)
            }
        }
    }

    companion object {
        fun bind(path: String): JvmUnixListener {
            if (path.isEmpty()) throw TransportException("Unix socket path is empty")
            val socketFile = Path.of(path)
            removeStaleSocket(socketFile, path)
            val server = ServerSocketChannel.open(StandardProtocolFamily.UNIX)
            try {
                server.bind(UnixDomainSocketAddress.of(socketFile))
                server.configureBlocking(false)
                val selector = Selector.open()
                server.register(selector, SelectionKey.OP_ACCEPT)
                return JvmUnixListener(server, selector, socketFile, path)
            } catch (e: Throwable) {
                server.close()
                throw e
            }
        }

        /**
         * Deletes [socketFile] only when it is a socket that refuses connections. Regular files and
         * live sockets are left alone and make the following bind fail.
         */
        private fun removeStaleSocket(
            socketFile: Path,
            path: String,
        ) {
            val attributes =
                try {
                    Files.readAttributes(socketFile, BasicFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
                } catch (ignored: IOException) {
                    return
                }
            if (!attributes.isOther) return
            if (isServed(socketFile)) throw TransportException("Unix socket $path is already in use")
            Files.deleteIfExists(socketFile)
        }

        private fun isServed(socketFile: Path): Boolean =
            try {
                SocketChannel.open(StandardProtocolFamily.UNIX).use { it.connect(UnixDomainSocketAddress.of(socketFile)) }
                true
            } catch (ignored: ConnectException) {
                false
            }
    }
}
