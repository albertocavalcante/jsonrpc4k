package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.io.File
import java.io.IOException
import java.io.InputStream
import kotlin.concurrent.thread
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val DEFAULT_EXIT_TIMEOUT: Duration = 2.seconds
private val TERMINATE_TIMEOUT: Duration = 1.seconds

/**
 * Where the standard error of a child started by [ProcessTransport.start] goes. Every mode keeps the child from
 * blocking on a full stderr pipe.
 */
public sealed interface StderrMode {
    /** The child writes to this process's standard error. */
    public data object Inherit : StderrMode

    /** The operating system discards the child's standard error. */
    public data object Discard : StderrMode

    /**
     * A daemon thread drains the child's standard error until EOF and passes each chunk to [onData], in order and on
     * that thread. Exceptions thrown by [onData] are ignored, so draining never stops.
     */
    public class Pipe(
        public val onData: (ByteArray) -> Unit,
    ) : StderrMode
}

/**
 * [Transport] over the standard input and output of a child process that it starts and owns, such as a BSP server
 * launched from a `.bsp` connection file.
 *
 * [incoming] reads the child's stdout and [send] writes its stdin, with [StreamTransport] semantics. Sessions never
 * close transports, so the owner calls [close]. It closes the child's stdin so the child sees EOF, waits up to the
 * exit timeout for it to exit, then destroys it (`SIGTERM` on Unix), then destroys it forcibly (`SIGKILL`), waiting
 * a bounded time after each step, and finally releases stdout without blocking. [close] therefore returns within the
 * exit timeout plus a few seconds, even when the child, or a grandchild that inherited its pipes, keeps running. Only
 * the child itself is ended: while such a grandchild holds the child's stdout, [incoming] can stay open until the
 * grandchild exits.
 */
public class ProcessTransport private constructor(
    private val process: Process,
    private val exitTimeout: Duration,
) : Transport {
    private val streams = StreamTransport(process.inputStream, process.outputStream)
    private val exit = CompletableDeferred<Int>()

    init {
        process.onExit().whenComplete { exited, failure ->
            if (failure == null) exit.complete(exited.exitValue()) else exit.completeExceptionally(failure)
        }
    }

    /** Operating-system id of the child process. */
    public val pid: Long = process.pid()

    override val incoming: Flow<ByteArray> = streams.incoming

    /** False after EOF, after [close], or once the child has exited. */
    override val isConnected: Boolean
        get() = streams.isConnected && process.isAlive

    override suspend fun send(data: ByteArray): Unit = streams.send(data)

    override suspend fun close(): Unit = streams.close { terminate() }

    /**
     * Suspends until the child exits and returns its exit code; it does not close the transport. On Unix a child
     * ended by a signal reports 128 plus the signal number, such as 143 after `SIGTERM` and 137 after `SIGKILL`.
     */
    public suspend fun awaitExit(): Int = exit.await()

    /** Ends the child after its stdin was closed: exit timeout, then `destroy()`, then `destroyForcibly()`. */
    private suspend fun terminate(): Throwable? {
        if (exitsWithin(exitTimeout)) return null
        process.destroy()
        if (exitsWithin(TERMINATE_TIMEOUT)) return null
        process.destroyForcibly()
        return if (exitsWithin(TERMINATE_TIMEOUT)) null else TransportException("Process $pid did not exit after it was killed")
    }

    private suspend fun exitsWithin(timeout: Duration): Boolean = withTimeoutOrNull(timeout) { exit.join() } != null

    public companion object {
        /**
         * Starts [command] with piped stdin and stdout and returns a transport that owns the child.
         *
         * @param command program and arguments; the program is resolved as [ProcessBuilder] resolves it.
         * @param environment changes to the inherited environment: a value sets a variable and `null` removes it.
         * @param workingDirectory directory the child starts in, or `null` for this process's working directory.
         * @param stderr where the child's standard error goes.
         * @param exitTimeout how long [close] waits for the child to exit after closing its stdin before destroying
         *   it; [Duration.ZERO] destroys it at once.
         * @throws TransportException when the process cannot be started.
         */
        public suspend fun start(
            command: List<String>,
            environment: Map<String, String?> = emptyMap(),
            workingDirectory: String? = null,
            stderr: StderrMode = StderrMode.Inherit,
            exitTimeout: Duration = DEFAULT_EXIT_TIMEOUT,
        ): ProcessTransport {
            require(command.isNotEmpty()) { "command must not be empty" }
            require(!exitTimeout.isNegative()) { "exitTimeout must not be negative" }
            val builder = processBuilder(command, environment, workingDirectory, stderr)
            val context = currentCoroutineContext()
            context.ensureActive()
            // Not cancellable: once started, the child is owned, so a caller cancelled meanwhile still ends it below.
            val transport =
                withContext(NonCancellable + Dispatchers.IO) {
                    val process = startProcess(builder)
                    if (stderr is StderrMode.Pipe) drainStderr(process, stderr.onData)
                    ProcessTransport(process, exitTimeout)
                }
            if (!context.isActive) {
                transport.close()
                context.ensureActive()
            }
            return transport
        }
    }
}

private fun processBuilder(
    command: List<String>,
    environment: Map<String, String?>,
    workingDirectory: String?,
    stderr: StderrMode,
): ProcessBuilder {
    val builder = ProcessBuilder(command)
    val variables = builder.environment()
    for ((name, value) in environment) {
        if (value == null) {
            variables.remove(name)
        } else {
            variables[name] = value
        }
    }
    if (workingDirectory != null) builder.directory(File(workingDirectory))
    builder.redirectError(
        when (stderr) {
            StderrMode.Inherit -> ProcessBuilder.Redirect.INHERIT
            StderrMode.Discard -> ProcessBuilder.Redirect.DISCARD
            is StderrMode.Pipe -> ProcessBuilder.Redirect.PIPE
        },
    )
    return builder
}

private fun startProcess(builder: ProcessBuilder): Process =
    try {
        builder.start()
    } catch (e: IOException) {
        throw TransportException("Failed to start process ${builder.command().first()}", e)
    }

/** Drains the child's stderr pipe on a daemon thread until EOF so that the pipe never fills. */
private fun drainStderr(
    process: Process,
    onData: (ByteArray) -> Unit,
) {
    val stream = process.errorStream
    thread(isDaemon = true, name = "jsonrpc4k-stderr-${process.pid()}") {
        drain(stream, onData)
    }
}

private fun drain(
    stream: InputStream,
    onData: (ByteArray) -> Unit,
) {
    try {
        val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
        var count = readOrEnd(stream, buffer)
        while (count >= 0) {
            if (count > 0) deliver(onData, buffer.copyOf(count))
            count = readOrEnd(stream, buffer)
        }
    } finally {
        closeQuietly(stream)
    }
}

/** Returns the number of bytes read, or -1 at EOF or when the pipe failed or was closed. */
private fun readOrEnd(
    stream: InputStream,
    buffer: ByteArray,
): Int =
    try {
        stream.read(buffer)
    } catch (ignored: IOException) {
        -1
    }

private fun deliver(
    onData: (ByteArray) -> Unit,
    chunk: ByteArray,
) {
    try {
        onData(chunk)
    } catch (ignored: Exception) {
        // A failing callback must not stop draining, or the child could block on a full pipe.
    }
}

private fun closeQuietly(stream: InputStream) {
    try {
        stream.close()
    } catch (ignored: IOException) {
        // The drain is over; there is nobody to report to.
    }
}
