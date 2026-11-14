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
import kotlinx.coroutines.yield
import platform.posix.EINTR
import platform.posix.FIONBIO
import platform.posix.POLLERR
import platform.posix.POLLHUP
import platform.posix.POLLNVAL
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
import platform.windows.Sleep
import platform.windows.connect
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import platform.windows.socket
import kotlin.concurrent.AtomicInt
import kotlin.time.TimeSource

// Constants for Windows sockets
internal const val SOCKET_ERROR_VALUE = -1
internal val INVALID_SOCKET_VALUE = ULong.MAX_VALUE

// How long a waiting read or a write to a full send buffer sleeps in WSAPoll() before it re-checks close().
private const val SOCKET_POLL_MILLIS = 100

// revents that let a read make progress: data, or an EOF, reset or invalid handle that recv then reports.
private const val READ_READY = POLLRDNORM or POLLHUP or POLLERR or POLLNVAL

// revents that let an accept make progress: a pending connection, or an error that accept then reports.
internal const val ACCEPT_READY = POLLRDNORM or POLLERR or POLLNVAL

// revents that let a write make progress. POLLHUP is left out: WSAPoll reports it once the peer has shut down its
// sending side, whatever was requested, while that peer may still read and the send buffer may still be full.
private const val WRITE_READY = POLLWRNORM or POLLERR or POLLNVAL

// Returned by a poll slice that found nothing to read.
private val NO_DATA = ByteArray(0)

// Largest single send() call. On windows-latest one non-blocking send of 64 MiB to a peer that did not read ended
// at once instead of waiting for buffer space; one default send buffer per call keeps the waits in poll slices.
private const val MAX_SEND_BYTES = 64 * 1024

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
 * and the caller's cancellation in between: a collector that stops early (for example `first`) or is cancelled
 * returns within a slice, and [close] never waits on the peer. The transport stays open afterwards, but the bytes
 * already read for that collector are dropped: the chunk of the slice in progress and any chunks that `flowOn`
 * buffered ahead of it. Blocking `recv` and `send` would not do: Winsock's `shutdown` does not end them, and only
 * `closesocket` from another thread does.
 *
 * Cost of the slices: a collector, or a send waiting for buffer space, occupies one [Dispatchers.IO] thread for as long
 * as it waits and wakes up ten times per second while the peer is quiet. `Dispatchers.IO` allows 64 threads by
 * default, so beyond that many concurrently waiting transports the slices take turns and latency grows. A `WSAPoll`
 * failure ends [incoming] or fails [send] with [TransportException] instead of being retried.
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
                    // After a quiet slice, let other work queued on the IO pool run before the next one.
                    if (chunk.isNotEmpty()) emit(chunk) else yield()
                }
            }
        }.flowOn(Dispatchers.IO)

    /** Returns the next chunk, [NO_DATA] after a quiet poll slice, or null at EOF or once closed. */
    private fun readStep(buffer: CPointer<ByteVar>): ByteArray? {
        if (closing.value != 0 || eof.value != 0) return null
        val readable =
            try {
                pollSocket(socketHandle, POLLRDNORM, READ_READY, SOCKET_POLL_MILLIS)
            } catch (e: TransportException) {
                return failRead(e)
            }
        if (!readable) return NO_DATA
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
        return failRead(TransportException("Read failed: socket error $error"))
    }

    /** Ends incoming: quietly once closing, otherwise by marking EOF and throwing [failure]. */
    private fun failRead(failure: TransportException): ByteArray? {
        if (closing.value != 0) return null
        eof.value = 1
        throw failure
    }

    override suspend fun send(data: ByteArray) {
        if (!isConnected) throw TransportException("Transport is closed")
        withContext(Dispatchers.IO) {
            writeLock.withLock {
                data.usePinned { pinned ->
                    var offset = 0
                    while (offset < data.size) {
                        if (closing.value != 0) throw TransportException("Transport is closed")
                        offset += sendSome(pinned.addressOf(offset), minOf(data.size - offset, MAX_SEND_BYTES))
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
        pollSocket(socketHandle, POLLWRNORM, WRITE_READY, SOCKET_POLL_MILLIS)
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
            openOnIo {
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
internal fun setNonBlocking(handle: ULong): Boolean =
    memScoped {
        val enabled = alloc<UIntVar>()
        enabled.value = 1u
        ioctlsocket(handle, FIONBIO.convert(), enabled.ptr) != SOCKET_ERROR_VALUE
    }

/** One `WSAPoll` call on a single entry; on failure it returns [SOCKET_ERROR_VALUE] and sets `WSAGetLastError`. */
@OptIn(ExperimentalForeignApi::class)
internal fun wsaPollOne(
    entry: CPointer<pollfd>,
    timeoutMillis: Int,
): Int = WSAPoll(entry, 1u, timeoutMillis)

/**
 * Waits at most [timeoutMillis] for [events] on [handle] and returns true when the socket reports any of [ready].
 * When `WSAPoll` returns early with only other bits (a lasting `POLLHUP`, say) or is interrupted (`WSAEINTR`), it
 * sleeps out the rest of the slice and returns false, so a caller that retries never spins. Any other `WSAPoll`
 * failure throws [TransportException]: it does not go away on retry, and retrying it would idle forever. [poll] is
 * the system call, replaced only by tests.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun pollSocket(
    handle: ULong,
    events: Int,
    ready: Int,
    timeoutMillis: Int,
    poll: (CPointer<pollfd>, Int) -> Int = ::wsaPollOne,
): Boolean {
    val start = TimeSource.Monotonic.markNow()
    val reported =
        memScoped {
            val entry = alloc<pollfd>()
            entry.fd = handle
            entry.events = events.convert()
            entry.revents = 0
            val result = poll(entry.ptr, timeoutMillis)
            if (result == SOCKET_ERROR_VALUE) {
                val error = WSAGetLastError()
                if (error != WSAEINTR) throw TransportException("Poll failed: socket error $error")
            }
            if (result > 0) entry.revents.toInt() else 0
        }
    if ((reported and ready) != 0) return true
    val left = timeoutMillis - start.elapsedNow().inWholeMilliseconds
    if (left > 0) Sleep(left.convert())
    return false
}
