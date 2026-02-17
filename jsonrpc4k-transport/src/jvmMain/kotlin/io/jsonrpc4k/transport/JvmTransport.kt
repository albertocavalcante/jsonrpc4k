package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import java.io.InputStream
import java.io.OutputStream
import java.net.Socket
import java.util.concurrent.atomic.AtomicBoolean

/**
 * JVM implementation of Transport using InputStreams and OutputStreams.
 */
public class StreamTransport(
    private val input: InputStream,
    private val output: OutputStream,
    private val bufferSize: Int = 8192,
) : Transport {
    private val connected = AtomicBoolean(true)

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

    override suspend fun close() {
        if (connected.compareAndSet(true, false)) {
            withContext(Dispatchers.IO) {
                runCatching { input.close() }
                runCatching { output.close() }
            }
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
    override suspend fun close() {
        withContext(Dispatchers.IO) {
            runCatching { socket.close() }
        }
    }

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
