package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.UIntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import platform.posix.EINTR
import platform.posix.FIONBIO
import platform.posix.POLLRDNORM
import platform.posix.POLLWRNORM
import platform.posix.SD_SEND
import platform.posix.WSAEINTR
import platform.posix.WSAEWOULDBLOCK
import platform.posix.WSAGetLastError
import platform.posix.WSAPoll
import platform.posix._O_BINARY
import platform.posix._setmode
import platform.posix.closesocket
import platform.posix.errno
import platform.posix.init_sockets
import platform.posix.ioctlsocket
import platform.posix.pollfd
import platform.posix.read
import platform.posix.recv
import platform.posix.send
import platform.posix.shutdown
import platform.posix.write
import platform.windows.ADDRINFOA
import platform.windows.AF_INET
import platform.windows.SOCK_STREAM
import platform.windows.connect
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import platform.windows.socket
import kotlin.concurrent.AtomicInt

// Constants for Windows sockets
internal const val SOCKET_ERROR_VALUE = -1
internal val INVALID_SOCKET_VALUE = ULong.MAX_VALUE

// How long a waiting read or a write to a full send buffer sleeps in WSAPoll() before it re-checks close().
private const val SOCKET_POLL_MILLIS = 100

// Returned by a poll slice that found nothing to read.
private val NO_DATA = ByteArray(0)

// CRT file descriptors for the process stdio streams, and the _setmode failure value.
private const val STDIN_FD = 0
private const val STDOUT_FD = 1
private const val SETMODE_FAILED = -1

/**
 * Switches CRT fds 0 and 1 to binary mode exactly once per process. The MinGW CRT opens them in
 * text mode, which translates `\n` <-> `\r\n` and treats Ctrl-Z (0x1A) as end of file, corrupting
 * Content-Length framed bodies. A failed attempt is retried by the next stdio transport.
 */
private val binaryStdio: Lazy<Unit> =
    lazy {
        setBinaryMode(STDIN_FD)
        setBinaryMode(STDOUT_FD)
    }

private fun ensureBinaryStdio(): Unit = binaryStdio.value

@OptIn(ExperimentalForeignApi::class)
private fun setBinaryMode(fd: Int) {
    if (_setmode(fd, _O_BINARY) == SETMODE_FAILED) {
        throw TransportException("Failed to set binary mode on fd $fd: error $errno")
    }
}

// Thread-safe, once-only WSAStartup; a failed attempt is retried by the next caller.
private val wsaStartup: Lazy<Unit> =
    lazy {
        val result = init_sockets()
        if (result != 0) {
            throw TransportException("WSAStartup failed: error $result")
        }
    }

internal fun initWinsock(): Unit = wsaStartup.value

/**
 * Create the platform-specific stdio transport for Windows.
 */
@OptIn(ExperimentalForeignApi::class)
internal actual fun createStdioTransport(): Transport = MingwStdioTransport()

/**
 * Create the platform-specific socket transport for Windows.
 */
internal actual suspend fun connectTcp(
    host: String,
    port: Int,
): Transport = MingwSocketTransport.connect(host, port)

/**
 * Windows implementation of Transport using file descriptors for stdio.
 * Uses POSIX-compatible read/write for console I/O.
 */
@OptIn(ExperimentalForeignApi::class)
internal class MingwStdioTransport(
    private val bufferSize: Int = 8192,
) : Transport {
    // Use POSIX file descriptors for stdio (0 = stdin, 1 = stdout)
    private val readFd = STDIN_FD
    private val writeFd = STDOUT_FD

    init {
        ensureBinaryStdio()
    }

    // 0 = connected, 1 = disconnected
    private val connected = AtomicInt(0)

    override val incoming: Flow<ByteArray> =
        flow {
            memScoped {
                val buffer = allocArray<ByteVar>(bufferSize)
                while (connected.value == 0) {
                    val bytesRead = read(readFd, buffer, bufferSize.toUInt())

                    when {
                        bytesRead < 0 -> {
                            val err = errno
                            if (err == EINTR) continue
                            if (connected.value == 0) {
                                connected.compareAndSet(0, 1)
                                throw TransportException("Read failed: error $err")
                            }
                            break
                        }
                        bytesRead == 0 -> {
                            connected.compareAndSet(0, 1)
                            break
                        }
                        else -> {
                            val data = ByteArray(bytesRead) { i -> buffer[i] }
                            emit(data)
                        }
                    }
                }
            }
        }.flowOn(Dispatchers.Default)

    override suspend fun send(data: ByteArray): Unit =
        withContext(Dispatchers.Default) {
            if (connected.value != 0) {
                throw TransportException("Transport is closed")
            }

            data.usePinned { pinned ->
                var offset = 0
                while (offset < data.size) {
                    val bytesWritten =
                        write(
                            writeFd,
                            pinned.addressOf(offset),
                            (data.size - offset).toUInt(),
                        )

                    when {
                        bytesWritten < 0 -> {
                            val err = errno
                            if (err == EINTR) continue
                            throw TransportException("Write failed: error $err")
                        }
                        bytesWritten == 0 -> {
                            throw TransportException("Write returned 0 bytes")
                        }
                        else -> {
                            offset += bytesWritten
                        }
                    }
                }
            }
        }

    override suspend fun close() {
        connected.compareAndSet(0, 1)
        // Don't close stdin/stdout
    }

    override val isConnected: Boolean
        get() = connected.value == 0
}

/**
 * Socket transport for Windows using Winsock2.
 *
 * The socket is non-blocking and every wait is a `WSAPoll()` slice of [SOCKET_POLL_MILLIS] that re-checks [close]
 * and the collector's cancellation in between: a collector that stops early (for example `first`) or is cancelled
 * returns within a slice, and [close] never waits on the peer. Blocking `recv` and `send` would not do: Winsock's
 * `shutdown` does not end them, and only `closesocket` from another thread does.
 *
 * [close] works from a cancelled coroutine. It sends FIN with `shutdown(SD_SEND)` at once, so the peer sees EOF
 * after the bytes already sent, and then closes the socket under the read and write locks, so a concurrent read or
 * write never reaches a handle that was closed and reused. EOF does not release the socket; [close] does.
 */
@OptIn(ExperimentalForeignApi::class)
internal class MingwSocketTransport internal constructor(
    private val socketHandle: ULong,
    private val bufferSize: Int = 8192,
) : Transport {
    // 0 = open, 1 = close() was called
    private val closing = AtomicInt(0)

    // 0 = reading, 1 = EOF or a read failure was seen
    private val eof = AtomicInt(0)
    private val readLock = Mutex()
    private val writeLock = Mutex()

    init {
        // When this throws, the caller still owns the handle and closes it.
        if (!setNonBlocking(socketHandle)) {
            throw TransportException("Failed to make the socket non-blocking: socket error ${WSAGetLastError()}")
        }
    }

    override val incoming: Flow<ByteArray> =
        flow {
            memScoped {
                val buffer = allocArray<ByteVar>(bufferSize)
                while (true) {
                    // Poll slices never suspend, so a cancelled collector (for example `first`) is noticed here.
                    currentCoroutineContext().ensureActive()
                    val chunk = readLock.withLock { readStep(buffer) } ?: break
                    if (chunk.isNotEmpty()) emit(chunk)
                }
            }
        }.flowOn(Dispatchers.IO)

    /** Returns the next chunk, [NO_DATA] after a quiet poll slice, or null at EOF or once closed. */
    private fun readStep(buffer: CPointer<ByteVar>): ByteArray? {
        if (closing.value != 0 || eof.value != 0) return null
        if (!pollSocket(socketHandle, POLLRDNORM, SOCKET_POLL_MILLIS)) return NO_DATA
        val count = recv(socketHandle, buffer, bufferSize, 0)
        return when {
            // Closing ends incoming quietly, including a read that returns only after close().
            count > 0 -> if (closing.value == 0) ByteArray(count) { buffer[it] } else null
            count == 0 -> {
                eof.value = 1
                null
            }
            else -> readFailure(WSAGetLastError())
        }
    }

    private fun readFailure(error: Int): ByteArray? {
        if (error == WSAEWOULDBLOCK || error == WSAEINTR) return NO_DATA
        if (closing.value != 0) return null
        eof.value = 1
        throw TransportException("Read failed: socket error $error")
    }

    override suspend fun send(data: ByteArray) {
        if (!isConnected) throw TransportException("Transport is closed")
        withContext(Dispatchers.IO) {
            writeLock.withLock {
                data.usePinned { pinned ->
                    var offset = 0
                    while (offset < data.size) {
                        if (closing.value != 0) throw TransportException("Transport is closed")
                        offset += sendSome(pinned.addressOf(offset), data.size - offset)
                    }
                }
            }
        }
    }

    /** Sends at most [length] bytes and returns how many were sent; 0 after a poll slice while the buffer is full. */
    private fun sendSome(
        start: CPointer<ByteVar>,
        length: Int,
    ): Int {
        val count = send(socketHandle, start, length, 0)
        if (count != SOCKET_ERROR_VALUE) return count
        val error = WSAGetLastError()
        if (error != WSAEWOULDBLOCK && error != WSAEINTR) {
            val reason = if (closing.value != 0) "Transport is closed" else "Write failed: socket error $error"
            throw TransportException(reason)
        }
        pollSocket(socketHandle, POLLWRNORM, SOCKET_POLL_MILLIS)
        return 0
    }

    override suspend fun close() {
        if (!closing.compareAndSet(0, 1)) return
        // Queues FIN after the bytes already sent, so the peer sees EOF even while a poll slice is still running.
        shutdown(socketHandle, SD_SEND)
        // Not cancellable, so the socket is released even when the caller is already cancelled. Each lock is held
        // for at most one poll slice.
        withContext(NonCancellable) {
            writeLock.withLock { readLock.withLock { closesocket(socketHandle) } }
        }
    }

    override val isConnected: Boolean
        get() = closing.value == 0 && eof.value == 0

    companion object {
        /**
         * Connect to a socket server.
         */
        suspend fun connect(
            host: String,
            port: Int,
        ): MingwSocketTransport =
            withContext(Dispatchers.Default) {
                initWinsock()

                memScoped {
                    // Use getaddrinfo for hostname resolution
                    val hints = alloc<ADDRINFOA>()
                    hints.ai_family = AF_INET
                    hints.ai_socktype = SOCK_STREAM

                    val resultPtr = alloc<CPointerVar<ADDRINFOA>>()
                    val portStr = port.toString()

                    val gaiResult = getaddrinfo(host, portStr, hints.ptr, resultPtr.ptr)
                    if (gaiResult != 0) {
                        throw TransportException("Failed to resolve host $host: error $gaiResult")
                    }

                    val result =
                        resultPtr.value
                            ?: throw TransportException("No address found for host: $host")

                    try {
                        // Create socket
                        val sockHandle =
                            socket(
                                result.pointed.ai_family,
                                result.pointed.ai_socktype,
                                result.pointed.ai_protocol,
                            )
                        if (sockHandle == INVALID_SOCKET_VALUE) {
                            throw TransportException("Failed to create socket: error ${WSAGetLastError()}")
                        }

                        try {
                            // Connect
                            val connectResult =
                                connect(
                                    sockHandle,
                                    result.pointed.ai_addr,
                                    result.pointed.ai_addrlen.convert(),
                                )

                            if (connectResult == SOCKET_ERROR_VALUE) {
                                throw TransportException(
                                    "Failed to connect to $host:$port: error ${WSAGetLastError()}",
                                )
                            }

                            MingwSocketTransport(sockHandle)
                        } catch (e: Exception) {
                            closesocket(sockHandle)
                            throw e
                        }
                    } finally {
                        freeaddrinfo(result)
                    }
                }
            }
    }
}

/** Switches [handle] to non-blocking mode; false when `ioctlsocket` fails, with the reason in `WSAGetLastError`. */
@OptIn(ExperimentalForeignApi::class)
private fun setNonBlocking(handle: ULong): Boolean =
    memScoped {
        val enabled = alloc<UIntVar>()
        enabled.value = 1u
        ioctlsocket(handle, FIONBIO.convert(), enabled.ptr) != SOCKET_ERROR_VALUE
    }

/**
 * Waits at most [timeoutMillis] for [events] on [handle]. True when the socket is ready, hung up or failed, and also
 * when `WSAPoll` itself fails: the socket is non-blocking, so the call that follows reports the error without waiting.
 */
@OptIn(ExperimentalForeignApi::class)
private fun pollSocket(
    handle: ULong,
    events: Int,
    timeoutMillis: Int,
): Boolean =
    memScoped {
        val entry = alloc<pollfd>()
        entry.fd = handle
        entry.events = events.convert()
        entry.revents = 0
        WSAPoll(entry.ptr, 1u, timeoutMillis) != 0
    }
