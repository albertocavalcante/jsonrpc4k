package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

/**
 * JVM implementation of Transport using InputStreams and OutputStreams.
 * The transport owns both streams; EOF does not dispose them automatically.
 */
public class StreamTransport(
    private val input: InputStream,
    private val output: OutputStream,
    private val bufferSize: Int = 8192,
) : Transport {
    private val connected = AtomicBoolean(true)
    private val closeMutex = Mutex()
    private var inputClosed = false
    private var outputClosed = false

    private data class CloseFailure(
        val cause: Throwable,
        val fatal: Boolean,
    )

    override val incoming: Flow<ByteArray> =
        flow {
            val buffer = ByteArray(bufferSize)
            while (connected.get()) {
                val bytesRead =
                    try {
                        input.read(buffer)
                    } catch (e: Exception) {
                        if (connected.get()) throw TransportException("Read failed", e)
                        break
                    }

                if (bytesRead < 0) {
                    connected.set(false)
                    break
                }

                if (bytesRead > 0) {
                    emit(buffer.copyOf(bytesRead))
                }
            }
        }.flowOn(Dispatchers.IO)

    override suspend fun send(data: ByteArray) {
        if (!connected.get()) {
            throw TransportException("Transport is closed")
        }
        withContext(Dispatchers.IO) {
            try {
                output.write(data)
                output.flush()
            } catch (e: Exception) {
                throw TransportException("Write failed", e)
            }
        }
    }

    /**
     * Disposes both owned streams, including after EOF or caller cancellation.
     * Concurrent callers wait for the same serialized cleanup. Failed resources may be retried;
     * successfully closed resources are not closed again. Close failures are reported with their causes.
     */
    override suspend fun close() {
        withContext(NonCancellable + Dispatchers.IO) {
            closeMutex.withLock {
                connected.set(false)
                closeStreams()
            }
        }
    }

    private fun closeStreams() {
        val inputFailure =
            if (inputClosed) {
                null
            } else {
                closeResource {
                    input.close()
                    inputClosed = true
                }
            }
        val outputFailure =
            if (outputClosed) {
                null
            } else {
                closeResource {
                    output.close()
                    outputClosed = true
                }
            }
        val failure = combineFailures(inputFailure, outputFailure) ?: return
        if (failure.fatal) throw failure.cause
        throw TransportException("Close failed", failure.cause)
    }

    private fun closeResource(close: () -> Unit): CloseFailure? =
        try {
            close()
            null
        } catch (failure: Exception) {
            CloseFailure(failure, fatal = false)
        } catch (fatal: Throwable) {
            CloseFailure(fatal, fatal = true)
        }

    private fun combineFailures(first: CloseFailure?, second: CloseFailure?): CloseFailure? =
        when {
            first == null -> second
            second == null || first.cause === second.cause -> first
            else -> {
                val primary = if (second.fatal && !first.fatal) second else first
                val secondary = if (primary === first) second else first
                primary.cause.addSuppressed(secondary.cause)
                primary
            }
        }

    override val isConnected: Boolean
        get() = connected.get()
}

/**
 * Stdio transport for JVM.
 * Uses System.in for reading and System.out for writing.
 */
public class StdioTransport :
    Transport by StreamTransport(
        input = System.`in`,
        output = System.out,
    )

/**
 * Socket transport for JVM.
 */
public class SocketTransport private constructor(
    private val socket: Socket,
) : Transport by StreamTransport(
        input = socket.getInputStream(),
        output = socket.getOutputStream(),
    ) {
    override val isConnected: Boolean
        get() = socket.isConnected && !socket.isClosed

    public companion object {
        /**
         * Connect to a socket server.
         */
        public suspend fun connect(
            host: String,
            port: Int,
        ): SocketTransport =
            withContext(Dispatchers.IO) {
                try {
                    val socket = Socket(host, port)
                    SocketTransport(socket)
                } catch (e: Exception) {
                    throw TransportException("Failed to connect to $host:$port", e)
                }
            }
    }
}

/**
 * JVM implementation of TransportFactory.
 */
public actual object TransportFactory {
    public actual fun stdio(): Transport = StdioTransport()

    public actual fun socket(
        host: String,
        port: Int,
    ): Transport {
        // Note: This is a blocking call, should be used with care
        // Prefer using SocketTransport.connect() for async connection
        return runBlocking { SocketTransport.connect(host, port) }
    }
}
