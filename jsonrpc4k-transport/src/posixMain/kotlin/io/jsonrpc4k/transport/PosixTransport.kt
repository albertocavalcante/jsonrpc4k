package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import platform.posix.AF_INET
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.EWOULDBLOCK
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

/**
 * POSIX implementation of Transport using file descriptors.
 *
 * @property shutdownOnClose when true (sockets only), [close] calls `shutdown(fd, SHUT_RDWR)`
 * before `close(fd)`. On Linux, `close()` alone does not wake a thread blocked in `read()` on
 * the same descriptor; `shutdown()` makes that read return 0 so [incoming] completes. Never
 * set this for stdio descriptors.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixFileDescriptorTransport(
    private val readFd: Int,
    private val writeFd: Int,
    private val closeOnComplete: Boolean = true,
    private val bufferSize: Int = 8192,
    private val shutdownOnClose: Boolean = false,
) : Transport {
    // 0 = connected, 1 = disconnected
    private val connected = AtomicInt(0)

    override val incoming: Flow<ByteArray> =
        flow {
            memScoped {
                val buffer = allocArray<ByteVar>(bufferSize)
                while (connected.value == 0) {
                    val bytesRead = read(readFd, buffer, bufferSize.convert())

                    when {
                        bytesRead < 0 -> {
                            val err = errno
                            if (err == EINTR) continue // Interrupted, retry
                            if (err == EAGAIN || err == EWOULDBLOCK) continue // Non-blocking would block
                            if (connected.value == 0) {
                                connected.compareAndSet(0, 1)
                                throw TransportException("Read failed: ${posixErrorMessage(err)}")
                            }
                            break
                        }
                        bytesRead == 0L -> {
                            // EOF
                            connected.compareAndSet(0, 1)
                            break
                        }
                        else -> {
                            val data = ByteArray(bytesRead.toInt()) { i -> buffer[i] }
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
                            (data.size - offset).convert(),
                        )

                    when {
                        bytesWritten < 0 -> {
                            val err = errno
                            if (err == EINTR) continue // Interrupted, retry
                            if (err == EAGAIN || err == EWOULDBLOCK) continue // Non-blocking would block
                            throw TransportException("Write failed: ${posixErrorMessage(err)}")
                        }
                        bytesWritten == 0L -> {
                            throw TransportException("Write returned 0 bytes")
                        }
                        else -> {
                            offset += bytesWritten.toInt()
                        }
                    }
                }
            }
        }

    override suspend fun close() {
        if (connected.compareAndSet(0, 1)) {
            if (closeOnComplete) {
                withContext(Dispatchers.Default) {
                    if (shutdownOnClose) {
                        // Wake any reader blocked in read() before releasing the descriptor.
                        shutdown(writeFd, SHUT_RDWR.convert())
                    }
                    if (readFd != writeFd) {
                        close(readFd)
                    }
                    close(writeFd)
                }
            }
        }
    }

    override val isConnected: Boolean
        get() = connected.value == 0
}

/**
 * Stdio transport for POSIX platforms.
 * Uses STDIN_FILENO for reading and STDOUT_FILENO for writing.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixStdioTransport :
    Transport by PosixFileDescriptorTransport(
        readFd = STDIN_FILENO,
        writeFd = STDOUT_FILENO,
        closeOnComplete = false, // Don't close stdin/stdout
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
        closeOnComplete = true,
        shutdownOnClose = true,
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
