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
import kotlinx.cinterop.toKString
import kotlinx.cinterop.value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import platform.posix.AF_INET
import platform.posix.EAGAIN
import platform.posix.ECONNABORTED
import platform.posix.EINTR
import platform.posix.POLLIN
import platform.posix.SHUT_RDWR
import platform.posix.SOCK_STREAM
import platform.posix.SOL_SOCKET
import platform.posix.SO_REUSEADDR
import platform.posix.accept
import platform.posix.addrinfo
import platform.posix.bind
import platform.posix.close
import platform.posix.errno
import platform.posix.freeaddrinfo
import platform.posix.gai_strerror
import platform.posix.getaddrinfo
import platform.posix.getsockname
import platform.posix.listen
import platform.posix.poll
import platform.posix.pollfd
import platform.posix.setsockopt
import platform.posix.shutdown
import platform.posix.sockaddr_in
import platform.posix.socket
import platform.posix.socklen_tVar
import kotlin.concurrent.AtomicInt
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.coroutineContext

private const val LISTEN_BACKLOG = 128

// How often a waiting accept wakes up to notice close() or coroutine cancellation.
private const val ACCEPT_POLL_MILLIS = 100

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun bindTcpListener(
    host: String,
    port: Int,
): TransportListener = PosixTcpListener.bind(host, port)

/**
 * TCP listener over a BSD listening socket.
 *
 * The descriptor is only closed once no [accept] is using it, so a concurrent `close()` can never
 * make an accepting thread operate on a recycled descriptor. `accept` waits in `poll()` with a short
 * timeout instead of blocking in `accept()`: `shutdown()` on a listening socket wakes a blocked
 * `accept()` on Linux but not on macOS, while the poll loop notices close and cancellation on both.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixTcpListener private constructor(
    private val fd: Int,
    override val port: Int,
) : TransportListener {
    // 0 = open, 1 = close requested
    private val closing = AtomicInt(0)

    // 0 = descriptor still open, 1 = descriptor released
    private val released = AtomicInt(0)

    private val acceptors = AtomicInt(0)

    override suspend fun accept(): Transport =
        withContext(Dispatchers.IO) {
            acceptors.incrementAndGet()
            try {
                acceptBlocking(coroutineContext)
            } finally {
                acceptors.decrementAndGet()
                releaseIfUnused()
            }
        }

    private fun acceptBlocking(context: CoroutineContext): PosixSocketTransport {
        while (true) {
            if (closing.value != 0) throw TransportException("Listener is closed")
            context.ensureActive()
            if (!waitReadable()) continue
            val client = accept(fd, null, null)
            if (client >= 0) {
                // A peer that closed its end must surface as EPIPE, not kill the process.
                ignoreSigpipeOnce()
                return PosixSocketTransport(client)
            }
            val err = errno
            if (closing.value != 0) throw TransportException("Listener is closed")
            if (err != EINTR && err != EAGAIN && err != ECONNABORTED) {
                throw TransportException("Accept failed: ${posixErrorMessage(err)}")
            }
        }
    }

    /** Returns true when the descriptor is readable (or hung up), false on timeout or EINTR. */
    private fun waitReadable(): Boolean =
        memScoped {
            val entry = alloc<pollfd>()
            entry.fd = fd
            entry.events = POLLIN.convert()
            entry.revents = 0
            val ready = poll(entry.ptr, 1.convert(), ACCEPT_POLL_MILLIS)
            if (ready < 0 && errno != EINTR && closing.value == 0) {
                throw TransportException("Accept failed: ${posixErrorMessage(errno)}")
            }
            ready > 0
        }

    override suspend fun close() {
        if (closing.compareAndSet(0, 1)) {
            // Wakes a blocked accept immediately on Linux; macOS answers ENOTCONN and relies on the poll loop.
            shutdown(fd, SHUT_RDWR.convert())
        }
        releaseIfUnused()
    }

    private fun releaseIfUnused() {
        if (closing.value != 0 && acceptors.value == 0 && released.compareAndSet(0, 1)) {
            close(fd)
        }
    }

    companion object {
        suspend fun bind(
            host: String,
            port: Int,
        ): PosixTcpListener =
            withContext(Dispatchers.IO) {
                ignoreSigpipeOnce()
                memScoped {
                    val hints = alloc<addrinfo>()
                    hints.ai_family = AF_INET
                    hints.ai_socktype = SOCK_STREAM

                    val resultPtr = alloc<CPointerVar<addrinfo>>()
                    val gaiResult = getaddrinfo(host, port.toString(), hints.ptr, resultPtr.ptr)
                    if (gaiResult != 0) {
                        val errorMsg = gai_strerror(gaiResult)?.toKString() ?: "Unknown error"
                        throw TransportException("Failed to resolve host $host: $errorMsg")
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
            info: addrinfo,
            host: String,
            port: Int,
        ): PosixTcpListener {
            val fd = socket(info.ai_family, info.ai_socktype, info.ai_protocol)
            if (fd < 0) {
                throw TransportException("Failed to create socket: ${posixErrorMessage(errno)}")
            }
            try {
                enableReuseAddress(fd)
                if (bind(fd, info.ai_addr, info.ai_addrlen) < 0) {
                    throw TransportException("Failed to listen on $host:$port: ${posixErrorMessage(errno)}")
                }
                if (listen(fd, LISTEN_BACKLOG) < 0) {
                    throw TransportException("Failed to listen on $host:$port: ${posixErrorMessage(errno)}")
                }
                return PosixTcpListener(fd, localPort(fd))
            } catch (e: Exception) {
                close(fd)
                throw e
            }
        }

        private fun enableReuseAddress(fd: Int) {
            memScoped {
                val enabled = alloc<IntVar>()
                enabled.value = 1
                if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, enabled.ptr, sizeOf<IntVar>().convert()) < 0) {
                    throw TransportException("Failed to set SO_REUSEADDR: ${posixErrorMessage(errno)}")
                }
            }
        }

        private fun localPort(fd: Int): Int =
            memScoped {
                val address = alloc<sockaddr_in>()
                val length = alloc<socklen_tVar>()
                length.value = sizeOf<sockaddr_in>().convert()
                if (getsockname(fd, address.ptr.reinterpret(), length.ptr) < 0) {
                    throw TransportException("Failed to read local port: ${posixErrorMessage(errno)}")
                }
                networkToHostPort(address.sin_port)
            }
    }
}
