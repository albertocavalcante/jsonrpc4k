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
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.withContext
import platform.posix.EINTR
import platform.posix.closesocket
import platform.posix.errno
import platform.posix.init_sockets
import platform.posix.read
import platform.posix.recv
import platform.posix.send
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
private const val SOCKET_ERROR_VALUE = -1
private val INVALID_SOCKET_VALUE = ULong.MAX_VALUE

/**
 * Create the platform-specific stdio transport for Windows.
 */
@OptIn(ExperimentalForeignApi::class)
internal actual fun createStdioTransport(): Transport = MingwStdioTransport()

/**
 * Create the platform-specific socket transport for Windows.
 */
internal actual suspend fun createSocketTransport(
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
    private val readFd = 0
    private val writeFd = 1

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
 */
@OptIn(ExperimentalForeignApi::class)
internal class MingwSocketTransport private constructor(
    private val socketHandle: ULong,
    private val bufferSize: Int = 8192,
) : Transport {
    // 0 = connected, 1 = disconnected
    private val connected = AtomicInt(0)

    override val incoming: Flow<ByteArray> =
        flow {
            memScoped {
                val buffer = allocArray<ByteVar>(bufferSize)
                while (connected.value == 0) {
                    val bytesRead = recv(socketHandle, buffer, bufferSize, 0)

                    when {
                        bytesRead < 0 -> {
                            if (connected.value == 0) {
                                connected.compareAndSet(0, 1)
                                throw TransportException("Read failed: socket error")
                            }
                            break
                        }
                        bytesRead == 0 -> {
                            // Connection closed
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
                        send(
                            socketHandle,
                            pinned.addressOf(offset).reinterpret(),
                            data.size - offset,
                            0,
                        )

                    when {
                        bytesWritten == SOCKET_ERROR_VALUE -> {
                            throw TransportException("Write failed: socket error")
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
        if (connected.compareAndSet(0, 1)) {
            withContext(Dispatchers.Default) {
                closesocket(socketHandle)
            }
        }
    }

    override val isConnected: Boolean
        get() = connected.value == 0

    companion object {
        private var wsaInitialized = false

        private fun initWsa() {
            if (!wsaInitialized) {
                val result = init_sockets()
                if (result != 0) {
                    throw TransportException("WSAStartup failed: $result")
                }
                wsaInitialized = true
            }
        }

        /**
         * Connect to a socket server.
         */
        suspend fun connect(
            host: String,
            port: Int,
        ): MingwSocketTransport =
            withContext(Dispatchers.Default) {
                initWsa()

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
                            throw TransportException("Failed to create socket")
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
                                throw TransportException("Failed to connect to $host:$port")
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
