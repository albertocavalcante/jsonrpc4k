package io.jsonrpc4k.transport

import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.sizeOf
import kotlinx.cinterop.value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import platform.posix.WSAGetLastError
import platform.posix.closesocket
import platform.windows.ADDRINFOA
import platform.windows.AF_INET
import platform.windows.SOCK_STREAM
import platform.windows.accept
import platform.windows.bind
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import platform.windows.getsockname
import platform.windows.listen
import platform.windows.sockaddr_in
import platform.windows.socket
import kotlin.concurrent.AtomicInt

private const val LISTEN_BACKLOG = 128

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun bindTcpListener(
    host: String,
    port: Int,
): TransportListener = MingwTcpListener.bind(host, port)

/**
 * TCP listener over a Winsock listening socket. `closesocket` from another thread makes a blocked
 * `accept` fail with an error, which is how [close] wakes a pending [accept]. A pending accept is not
 * interrupted by coroutine cancellation on Windows; call [close] to release it.
 *
 * SO_REUSEADDR is deliberately not set: on Windows it lets another process steal the port, and
 * rebinding after a close does not need it.
 */
@OptIn(ExperimentalForeignApi::class)
internal class MingwTcpListener private constructor(
    private val handle: ULong,
    override val port: Int,
) : TransportListener {
    // 0 = open, 1 = closed
    private val closed = AtomicInt(0)

    override suspend fun accept(): Transport =
        withContext(Dispatchers.IO) {
            if (closed.value != 0) throw TransportException("Listener is closed")
            val client = accept(handle, null, null)
            if (client == INVALID_SOCKET_VALUE) {
                if (closed.value != 0) throw TransportException("Listener is closed")
                throw TransportException("Accept failed: socket error ${WSAGetLastError()}")
            }
            MingwSocketTransport(client)
        }

    override suspend fun close() {
        if (closed.compareAndSet(0, 1)) {
            withContext(Dispatchers.IO) {
                closesocket(handle)
            }
        }
    }

    companion object {
        suspend fun bind(
            host: String,
            port: Int,
        ): MingwTcpListener =
            withContext(Dispatchers.IO) {
                initWinsock()
                memScoped {
                    val hints = alloc<ADDRINFOA>()
                    hints.ai_family = AF_INET
                    hints.ai_socktype = SOCK_STREAM

                    val resultPtr = alloc<CPointerVar<ADDRINFOA>>()
                    val gaiResult = getaddrinfo(host, port.toString(), hints.ptr, resultPtr.ptr)
                    if (gaiResult != 0) {
                        throw TransportException("Failed to resolve host $host: error $gaiResult")
                    }
                    val result = resultPtr.value ?: throw TransportException("No address found for host: $host")
                    try {
                        listenOn(result.pointed, host, port)
                    } finally {
                        freeaddrinfo(result)
                    }
                }
            }

        private fun listenOn(
            info: ADDRINFOA,
            host: String,
            port: Int,
        ): MingwTcpListener {
            val handle = socket(info.ai_family, info.ai_socktype, info.ai_protocol)
            if (handle == INVALID_SOCKET_VALUE) {
                throw TransportException("Failed to create socket: error ${WSAGetLastError()}")
            }
            try {
                if (bind(handle, info.ai_addr, info.ai_addrlen.convert()) == SOCKET_ERROR_VALUE ||
                    listen(handle, LISTEN_BACKLOG) == SOCKET_ERROR_VALUE
                ) {
                    throw TransportException("Failed to listen on $host:$port: error ${WSAGetLastError()}")
                }
                return MingwTcpListener(handle, localPort(handle))
            } catch (e: Exception) {
                closesocket(handle)
                throw e
            }
        }

        private fun localPort(handle: ULong): Int =
            memScoped {
                val address = alloc<sockaddr_in>()
                val length = alloc<IntVar>()
                length.value = sizeOf<sockaddr_in>().convert()
                if (getsockname(handle, address.ptr.reinterpret(), length.ptr) == SOCKET_ERROR_VALUE) {
                    throw TransportException("Failed to read local port: error ${WSAGetLastError()}")
                }
                networkToHostPort(address.sin_port)
            }
    }
}
