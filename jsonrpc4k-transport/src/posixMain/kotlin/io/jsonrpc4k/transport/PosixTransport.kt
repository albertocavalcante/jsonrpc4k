package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.toKString
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
import platform.posix.AF_INET
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
import platform.posix.POLLIN
import platform.posix.POLLOUT
import platform.posix.SHUT_RDWR
import platform.posix.SIGPIPE
import platform.posix.SIG_IGN
import platform.posix.SOCK_STREAM
import platform.posix.STDIN_FILENO
import platform.posix.STDOUT_FILENO
import platform.posix.addrinfo
import platform.posix.close
import platform.posix.connect
import platform.posix.errno
import platform.posix.freeaddrinfo
import platform.posix.gai_strerror
import platform.posix.getaddrinfo
import platform.posix.read
import platform.posix.shutdown
import platform.posix.signal
import platform.posix.socket
import platform.posix.strerror
import platform.posix.write
import kotlin.concurrent.AtomicInt

/**
 * Create the platform-specific stdio transport for POSIX platforms.
 */
@OptIn(ExperimentalForeignApi::class)
internal actual fun createStdioTransport(): Transport = PosixStdioTransport()

/**
 * Create the platform-specific socket transport for POSIX platforms.
 */
internal actual suspend fun connectTcp(
    host: String,
    port: Int,
): Transport = PosixSocketTransport.connect(host, port)

// How long a waiting read, or a write to a full non-blocking descriptor, sleeps in poll() before it re-checks close().
private const val READ_POLL_MILLIS = 100

// Returned by a poll slice that found nothing to read.
private val NO_DATA = ByteArray(0)

/**
 * POSIX implementation of [Transport] over file descriptors: a socket the transport owns, or stdin and stdout.
 *
 * A read waits in `poll()` slices of [READ_POLL_MILLIS] and re-checks [close] and the collector's cancellation
 * between slices, so a collector that stops early (for example `first`) or is cancelled returns within a slice and
 * the transport stays usable; a blocking `read()` would hold the collector until the peer sent more bytes.
 *
 * [close] works from a cancelled coroutine and never waits on the peer. For a socket it calls
 * `shutdown(SHUT_RDWR)` at once, which signals EOF to the peer, ends a pending read and fails a `write()` blocked
 * on a full send buffer, and then closes the descriptor under the read and write locks, so a concurrent read or
 * write never reaches a descriptor number that the process has reused. EOF does not release the socket; [close]
 * does.
 *
 * @property ownsSocket true when [readFd] and [writeFd] are one socket that [close] shuts down and closes; false
 * for stdio, whose descriptors [close] leaves open.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixFileDescriptorTransport(
    private val readFd: Int,
    private val writeFd: Int,
    private val ownsSocket: Boolean,
    private val bufferSize: Int = 8192,
) : Transport {
    // 0 = open, 1 = close() was called
    private val closing = AtomicInt(0)

    // 0 = reading, 1 = EOF or a read failure was seen
    private val eof = AtomicInt(0)
    private val readLock = Mutex()
    private val writeLock = Mutex()

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
        if (!pollFd(readFd, POLLIN.convert(), READ_POLL_MILLIS)) return NO_DATA
        val count = read(readFd, buffer, bufferSize.convert())
        return when {
            // Closing ends incoming quietly, including a read that returns only after close().
            count > 0 -> if (closing.value == 0) ByteArray(count.toInt()) { buffer[it] } else null
            count == 0L -> {
                eof.value = 1
                null
            }
            else -> readFailure(errno)
        }
    }

    private fun readFailure(err: Int): ByteArray? {
        if (err == EINTR || err == EAGAIN || err == EWOULDBLOCK) return NO_DATA
        if (closing.value != 0) return null
        eof.value = 1
        throw TransportException("Read failed: ${posixErrorMessage(err)}")
    }

    override suspend fun send(data: ByteArray) {
        if (!isConnected) throw TransportException("Transport is closed")
        withContext(Dispatchers.IO) {
            writeLock.withLock {
                data.usePinned { pinned ->
                    var offset = 0
                    while (offset < data.size) {
                        if (closing.value != 0) throw TransportException("Transport is closed")
                        offset += writeSome(pinned.addressOf(offset), data.size - offset)
                    }
                }
            }
        }
    }

    /** Writes at most [length] bytes and returns how many were written; 0 after EINTR or a full descriptor. */
    private fun writeSome(
        start: CPointer<ByteVar>,
        length: Int,
    ): Int {
        val count = write(writeFd, start, length.convert())
        if (count >= 0) return count.toInt()
        val err = errno
        if (err == EAGAIN || err == EWOULDBLOCK) {
            // Another process made the descriptor non-blocking (an inherited stdout pipe, say) and it is full.
            pollFd(writeFd, POLLOUT.convert(), READ_POLL_MILLIS)
        } else if (err != EINTR) {
            val reason = if (closing.value != 0) "Transport is closed" else "Write failed: ${posixErrorMessage(err)}"
            throw TransportException(reason)
        }
        return 0
    }

    override suspend fun close() {
        if (!closing.compareAndSet(0, 1) || !ownsSocket) return
        // Signals EOF to the peer now, ends a pending read and fails a write() blocked on a full send buffer.
        shutdown(writeFd, SHUT_RDWR.convert())
        // Not cancellable, so the socket is released even when the caller is already cancelled. A reader holds its
        // lock for at most one poll slice, and the shutdown above has just failed any blocked write.
        withContext(NonCancellable) {
            writeLock.withLock { readLock.withLock { close(writeFd) } }
        }
    }

    override val isConnected: Boolean
        get() = closing.value == 0 && eof.value == 0
}

/**
 * Stdio transport for POSIX platforms.
 * Uses STDIN_FILENO for reading and STDOUT_FILENO for writing; closing it leaves both open.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixStdioTransport :
    Transport by PosixFileDescriptorTransport(
        readFd = STDIN_FILENO,
        writeFd = STDOUT_FILENO,
        ownsSocket = false,
    )

/**
 * Socket transport for POSIX platforms using BSD sockets.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixSocketTransport internal constructor(
    socketFd: Int,
) : Transport by PosixFileDescriptorTransport(
        readFd = socketFd,
        writeFd = socketFd,
        ownsSocket = true,
    ) {
    companion object {
        /**
         * Connect to a socket server.
         */
        suspend fun connect(
            host: String,
            port: Int,
        ): PosixSocketTransport =
            withContext(Dispatchers.Default) {
                // A peer that closed its end must surface as EPIPE, not kill the process.
                ignoreSigpipeOnce()
                memScoped {
                    // Use getaddrinfo for proper hostname resolution
                    val hints = alloc<addrinfo>()
                    hints.ai_family = AF_INET
                    hints.ai_socktype = SOCK_STREAM

                    val resultPtr = alloc<CPointerVar<addrinfo>>()
                    val portStr = port.toString()

                    val gaiResult = getaddrinfo(host, portStr, hints.ptr, resultPtr.ptr)
                    if (gaiResult != 0) {
                        val errorMsg = gai_strerror(gaiResult)?.toKString() ?: "Unknown error"
                        throw TransportException("Failed to resolve host $host: $errorMsg")
                    }

                    val result =
                        resultPtr.value
                            ?: throw TransportException("No address found for host: $host")

                    try {
                        // Create socket
                        val sockFd = socket(result.pointed.ai_family, result.pointed.ai_socktype, result.pointed.ai_protocol)
                        if (sockFd < 0) {
                            throw TransportException("Failed to create socket: ${posixErrorMessage(errno)}")
                        }

                        try {
                            // Connect using the resolved address
                            val connectResult =
                                connect(
                                    sockFd,
                                    result.pointed.ai_addr,
                                    result.pointed.ai_addrlen,
                                )

                            if (connectResult < 0) {
                                throw TransportException("Failed to connect to $host:$port: ${posixErrorMessage(errno)}")
                            }

                            PosixSocketTransport(sockFd)
                        } catch (e: Exception) {
                            close(sockFd)
                            throw e
                        }
                    } finally {
                        freeaddrinfo(result)
                    }
                }
            }
    }
}

// 0 = SIGPIPE disposition not yet changed, 1 = set to SIG_IGN
private val sigpipeIgnored = AtomicInt(0)

/**
 * Ignore SIGPIPE process-wide, exactly once, before the first socket is created.
 *
 * Writing to a socket whose peer has closed raises SIGPIPE, whose default action terminates
 * the process. With the signal ignored, `write()` fails with `EPIPE` instead, which
 * [Transport.send] reports as a [TransportException].
 *
 * Tradeoff: this changes the disposition for the whole process (every broken-pipe write then
 * fails with `EPIPE`, and exec'd children inherit the ignored signal). The per-socket
 * alternatives are platform-specific (`send(..., MSG_NOSIGNAL)` on Linux, `SO_NOSIGPIPE` on
 * macOS) and would need separate Linux and Apple source sets; `posixMain` is shared by both,
 * and ignoring SIGPIPE is the common choice for networked native runtimes.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun ignoreSigpipeOnce() {
    if (sigpipeIgnored.compareAndSet(0, 1)) {
        signal(SIGPIPE, SIG_IGN)
    }
}

/**
 * Get error message from errno.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun posixErrorMessage(err: Int): String {
    val msg = strerror(err)
    return msg?.toKString() ?: "Unknown error ($err)"
}
