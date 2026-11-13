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

private val TERMINATE_TIMEOUT: Duration = 1.seconds

/**
 * JVM [ProcessTransport]: [incoming] reads the child's stdout and [send] writes its stdin, with [StreamTransport]
 * semantics; [close] follows the bounded sequence documented on [ProcessTransport].
 */
internal class JvmProcessTransport(
    private val process: Process,
    private val exitTimeout: Duration,
) : ProcessTransport {
    private val streams = StreamTransport(process.inputStream, process.outputStream)
    private val exit = CompletableDeferred<Int>()

    init {
        process.onExit().whenComplete { exited, failure ->
            if (failure == null) exit.complete(exited.exitValue()) else exit.completeExceptionally(failure)
        }
    }

    override val pid: Long = process.pid()

    override val incoming: Flow<ByteArray> = streams.incoming

    /** False after EOF, after [close], or once the child has exited. */
    override val isConnected: Boolean
        get() = streams.isConnected && process.isAlive

    override suspend fun send(data: ByteArray): Unit = streams.send(data)

    override suspend fun close(): Unit = streams.close { terminate() }

    override suspend fun awaitExit(): Int = exit.await()

    /** Ends the child after its stdin was closed: exit timeout, then `destroy()`, then `destroyForcibly()`. */
    private suspend fun terminate(): Throwable? {
        if (exitsWithin(exitTimeout)) return null
        process.destroy()
        if (exitsWithin(TERMINATE_TIMEOUT)) return null
        process.destroyForcibly()
        return if (exitsWithin(TERMINATE_TIMEOUT)) null else TransportException("Process $pid did not exit after it was killed")
    }

    private suspend fun exitsWithin(timeout: Duration): Boolean = withTimeoutOrNull(timeout) { exit.join() } != null
}

internal actual suspend fun startProcessTransport(
    command: List<String>,
    environment: Map<String, String?>,
    workingDirectory: String?,
    stderr: StderrMode,
    exitTimeout: Duration,
): ProcessTransport {
    val builder = processBuilder(command, environment, workingDirectory, stderr)
    val context = currentCoroutineContext()
    context.ensureActive()
    // Not cancellable: once started, the child is owned, so a caller cancelled meanwhile still ends it below.
    val transport =
        withContext(NonCancellable + Dispatchers.IO) {
            val process = startProcess(builder)
            if (stderr is StderrMode.Pipe) drainStderr(process, stderr.onData)
            JvmProcessTransport(process, exitTimeout)
        }
    if (!context.isActive) {
        transport.close()
        context.ensureActive()
    }
    return transport
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
