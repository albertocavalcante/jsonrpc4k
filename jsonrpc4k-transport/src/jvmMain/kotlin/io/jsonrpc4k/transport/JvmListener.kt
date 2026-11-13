package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import java.io.IOException
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.SocketTimeoutException
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

// How often a blocked accept() wakes up to notice coroutine cancellation.
private const val ACCEPT_POLL_MILLIS = 250

internal actual suspend fun bindTcpListener(
    host: String,
    port: Int,
): TransportListener = JvmTcpListener.bind(host, port)

/**
 * TCP listener backed by a [ServerSocket]. The listener owns the server socket; accepted sockets are
 * owned by the returned [SocketTransport].
 */
internal class JvmTcpListener private constructor(
    private val server: ServerSocket,
) : TransportListener {
    override val port: Int
        get() = server.localPort

    override suspend fun accept(): Transport =
        withContext(Dispatchers.IO) {
            acceptBlocking(coroutineContext)
        }

    private fun acceptBlocking(context: CoroutineContext): SocketTransport {
        while (true) {
            try {
                val socket = server.accept()
                // Do not inherit the accept poll timeout into the data connection.
                socket.soTimeout = 0
                return SocketTransport(socket)
            } catch (ignored: SocketTimeoutException) {
                context.ensureActive()
            } catch (e: IOException) {
                val reason = if (server.isClosed) "Listener is closed" else "Accept failed"
                throw TransportException(reason, e)
            }
        }
    }

    override suspend fun close() {
        withContext(NonCancellable + Dispatchers.IO) {
            try {
                server.close()
            } catch (e: IOException) {
                throw TransportException("Failed to close listener", e)
            }
        }
    }

    companion object {
        suspend fun bind(
            host: String,
            port: Int,
        ): JvmTcpListener =
            withContext(Dispatchers.IO) {
                val server = ServerSocket()
                try {
                    server.reuseAddress = true
                    server.soTimeout = ACCEPT_POLL_MILLIS
                    server.bind(InetSocketAddress(host, port))
                    JvmTcpListener(server)
                } catch (e: Exception) {
                    server.close()
                    throw TransportException("Failed to listen on $host:$port", e)
                }
            }
    }
}
