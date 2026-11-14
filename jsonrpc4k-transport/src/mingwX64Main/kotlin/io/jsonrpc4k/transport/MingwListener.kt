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
import kotlinx.coroutines.IO
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import platform.posix.POLLRDNORM
import platform.posix.WSAECONNRESET
import platform.posix.WSAEINTR
import platform.posix.WSAEWOULDBLOCK
import platform.posix.WSAGetLastError
import platform.posix.closesocket
import platform.posix.sockaddr_in
import platform.windows.ADDRINFOA
import platform.windows.AF_INET
import platform.windows.SOCK_STREAM
import platform.windows.accept
import platform.windows.bind
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import platform.windows.getsockname
import platform.windows.listen
import platform.windows.socket
import kotlin.concurrent.AtomicInt
import kotlin.coroutines.CoroutineContext

private const val LISTEN_BACKLOG = 128

// How often a waiting accept wakes up to notice close() or coroutine cancellation.
private const val ACCEPT_POLL_MILLIS = 100

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun bindTcpListener(
    host: String,
    port: Int,
): TransportListener = MingwTcpListener.bind(host, port)

/**
 * TCP listener over a non-blocking Winsock listening socket.
 *
 * [accept] waits in `WSAPoll()` slices of [ACCEPT_POLL_MILLIS] instead of blocking in `accept()`, so it notices
 * [close] and coroutine cancellation within a slice. Each pending accept occupies one `Dispatchers.IO` thread without
 * yielding it and wakes up ten times per second while no client connects, so 64 pending accepts (the default size
 * of `Dispatchers.IO`) starve all other IO work; see [MingwSocketTransport]. The handle is only closed once no
 * [accept] is using it, so a concurrent [close] can never make an accepting thread operate on a handle that Winsock
 * has since reused for another socket; a port with a pending accept is therefore released within one slice after
 * [close] returns.
 *
 * SO_REUSEADDR is deliberately not set: on Windows it lets another process steal the port, and
 * rebinding after a close does not need it.
 */
@OptIn(ExperimentalForeignApi::class)
internal class MingwTcpListener private constructor(
    private val handle: ULong,
    override val port: Int,
) : TransportListener {
    // 0 = open, 1 = close requested
    private val closing = AtomicInt(0)

    // 0 = handle still open, 1 = handle released
    private val released = AtomicInt(0)

    private val acceptors = AtomicInt(0)

    override suspend fun accept(): Transport =
        openOnIo { context ->
            // Counted before `closing` is checked, and close() sets `closing` before it checks the count, so the
            // handle is never released while this accept can still use it.
            acceptors.incrementAndGet()
            try {
                acceptBlocking(context)
            } finally {
                acceptors.decrementAndGet()
                releaseIfUnused()
            }
        }

    private fun acceptBlocking(context: CoroutineContext): MingwSocketTransport {
        while (true) {
            if (closing.value != 0) throw TransportException("Listener is closed")
            context.ensureActive()
            if (!pollSocket(handle, POLLRDNORM, ACCEPT_READY, ACCEPT_POLL_MILLIS)) continue
            val client = accept(handle, null, null)
            if (client != INVALID_SOCKET_VALUE) {
                // close() may have returned after the check above; it promises that a pending accept fails.
                if (closing.value != 0) {
                    closesocket(client)
                    throw TransportException("Listener is closed")
                }
                return adoptSocket(client)
            }
            checkAcceptError(WSAGetLastError())
        }
    }

    private fun checkAcceptError(error: Int) {
        if (closing.value != 0) throw TransportException("Listener is closed")
        // The pending connection was reset before it was accepted, or another accept took it first.
        if (error != WSAEWOULDBLOCK && error != WSAEINTR && error != WSAECONNRESET) {
            throw TransportException("Accept failed: socket error $error")
        }
    }

    override suspend fun close() {
        // Runs in place rather than through withContext, which would throw before the call when the caller is
        // already cancelled and leave the port bound.
        closing.compareAndSet(0, 1)
        releaseIfUnused()
    }

    private fun releaseIfUnused() {
        if (closing.value != 0 && acceptors.value == 0 && released.compareAndSet(0, 1)) {
            closesocket(handle)
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
                bindAndListen(handle, info, "$host:$port")
                return MingwTcpListener(handle, localPort(handle))
            } catch (e: Exception) {
                closesocket(handle)
                throw e
            }
        }

        private fun bindAndListen(
            handle: ULong,
            info: ADDRINFOA,
            target: String,
        ) {
            if (bind(handle, info.ai_addr, info.ai_addrlen.convert()) == SOCKET_ERROR_VALUE ||
                listen(handle, LISTEN_BACKLOG) == SOCKET_ERROR_VALUE
            ) {
                throw TransportException("Failed to listen on $target: error ${WSAGetLastError()}")
            }
            // accept() waits in WSAPoll slices, so it must never block on a connection that was reset meanwhile.
            if (!setNonBlocking(handle)) {
                throw TransportException("Failed to make the listener non-blocking: socket error ${WSAGetLastError()}")
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

/** A transport that owns the accepted socket [client]; [client] is closed when the transport cannot be set up. */
@OptIn(ExperimentalForeignApi::class)
private fun adoptSocket(client: ULong): MingwSocketTransport =
    try {
        MingwSocketTransport(client)
    } catch (e: TransportException) {
        closesocket(client)
        throw e
    }
