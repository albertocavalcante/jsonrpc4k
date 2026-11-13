package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import java.io.FilterInputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.PrintStream
import java.net.Socket
import java.net.SocketTimeoutException
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val DEFAULT_CLOSE_TIMEOUT: Duration = 1.seconds

/**
 * JVM [Transport] over an [InputStream] and an [OutputStream], both owned by the transport.
 *
 * EOF does not dispose the streams; call [close]. Concurrent [send] calls are serialized, so each payload is written
 * contiguously. [close] marks the transport closed, then closes the output first, so the peer sees EOF, and the input
 * second. Each stream is closed on a daemon thread and awaited for at most `closeTimeout`: a stream whose close
 * blocks, such as a process pipe or console held by a blocked reader or writer, finishes closing in the background
 * and a later [close] reports its outcome, so [close] never waits indefinitely for a live peer. A chunk that a
 * blocked read returns after [close] is dropped.
 *
 * @param bufferSize maximum number of bytes read from the input at once.
 * @param closeTimeout longest wait for each stream to close; defaults to one second.
 */
public class StreamTransport(
    private val input: InputStream,
    private val output: OutputStream,
    private val bufferSize: Int = DEFAULT_BUFFER_SIZE,
    private val closeTimeout: Duration = DEFAULT_CLOSE_TIMEOUT,
) : Transport {
    private val connected = AtomicBoolean(true)
    private val sendMutex = Mutex()
    private val closeMutex = Mutex()
    private val outputCloser = BoundedCloser(output, "jsonrpc4k-close-output")
    private val inputCloser = BoundedCloser(input, "jsonrpc4k-close-input")

    init {
        require(bufferSize > 0) { "bufferSize must be positive" }
        require(!closeTimeout.isNegative()) { "closeTimeout must not be negative" }
    }

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
                    } catch (ignored: SocketTimeoutException) {
                        // Read timeouts exist so a cancelled collector can stop; keep polling otherwise.
                        currentCoroutineContext().ensureActive()
                        continue
                    } catch (e: Exception) {
                        if (connected.get()) throw TransportException("Read failed", e)
                        break
                    }

                if (bytesRead < 0) {
                    connected.set(false)
                    break
                }

                // Closing ends incoming quietly, including a read that returns only after close().
                if (bytesRead > 0 && connected.get()) {
                    emit(buffer.copyOf(bytesRead))
                }
            }
        }.flowOn(Dispatchers.IO)

    override suspend fun send(data: ByteArray) {
        ensureOpen()
        sendMutex.withLock {
            ensureOpen()
            withContext(Dispatchers.IO) {
                try {
                    output.write(data)
                    output.flush()
                } catch (e: Exception) {
                    throw TransportException("Write failed", e)
                }
            }
        }
    }

    /**
     * Disposes both owned streams, including after EOF or caller cancellation.
     * Concurrent callers wait for the same serialized cleanup. Failed resources are retried by the next call;
     * successfully closed resources are not closed again. Close failures are reported with their causes.
     */
    override suspend fun close(): Unit = close { null }

    /**
     * Runs [close] with [beforeInput] between closing the output and closing the input; [ProcessTransport] ends its
     * child there. A failure that [beforeInput] returns is reported together with the stream failures.
     */
    internal suspend fun close(beforeInput: suspend () -> Throwable?) {
        withContext(NonCancellable + Dispatchers.IO) {
            closeMutex.withLock {
                connected.set(false)
                val outputFailure = outputCloser.close(closeTimeout)?.toCloseFailure()
                val peerFailure = beforeInput()?.toCloseFailure()
                val inputFailure = inputCloser.close(closeTimeout)?.toCloseFailure()
                val failure = combineFailures(combineFailures(inputFailure, outputFailure), peerFailure)
                if (failure != null) {
                    if (failure.fatal) throw failure.cause
                    throw TransportException("Close failed", failure.cause)
                }
            }
        }
    }

    private fun ensureOpen() {
        if (!connected.get()) throw TransportException("Transport is closed")
    }

    private fun Throwable.toCloseFailure(): CloseFailure = CloseFailure(this, fatal = this !is Exception)

    private fun combineFailures(
        first: CloseFailure?,
        second: CloseFailure?,
    ): CloseFailure? =
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
 * Stdio transport for JVM: reads [System.in] and writes [System.out].
 *
 * The process's standard streams are not owned: closing the transport stops it without closing them, like the native
 * stdio transports. A read blocked on stdin returns only when input or EOF arrives, and its chunk is then dropped.
 * Nothing else may write to [System.out] while the transport is in use, or the framing breaks.
 */
public class StdioTransport :
    Transport by StreamTransport(
        input = UnownedInputStream(System.`in`),
        output = UnownedPrintStream(System.out),
    )

/** View of an input stream that the transport reads but does not own, such as [System.in]: closing it does nothing. */
private class UnownedInputStream(
    input: InputStream,
) : FilterInputStream(input) {
    override fun close(): Unit = Unit
}

/**
 * View of a [PrintStream] that the transport writes but does not own, such as [System.out]: closing it does nothing,
 * and [flush] reports the failure that [PrintStream] records instead of throwing as an [IOException].
 */
private class UnownedPrintStream(
    private val out: PrintStream,
) : OutputStream() {
    override fun write(b: Int): Unit = out.write(b)

    override fun write(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Unit = out.write(b, off, len)

    override fun flush() {
        if (out.checkError()) throw IOException("Standard output failed")
    }

    override fun close(): Unit = Unit
}

private const val READ_POLL_MILLIS = 100

/**
 * A blocking socket read cannot be interrupted by coroutine cancellation, so reads time out
 * periodically and [StreamTransport] re-checks cancellation between attempts.
 */
private fun pollingStream(socket: Socket): StreamTransport {
    socket.soTimeout = READ_POLL_MILLIS
    return StreamTransport(input = socket.getInputStream(), output = socket.getOutputStream())
}

/**
 * Socket transport for JVM.
 */
public class SocketTransport internal constructor(
    private val socket: Socket,
    private val stream: StreamTransport = pollingStream(socket),
) : Transport by stream {
    override val isConnected: Boolean
        get() = socket.isConnected && !socket.isClosed && stream.isConnected

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
}

internal actual suspend fun connectTcp(
    host: String,
    port: Int,
): Transport = SocketTransport.connect(host, port)
