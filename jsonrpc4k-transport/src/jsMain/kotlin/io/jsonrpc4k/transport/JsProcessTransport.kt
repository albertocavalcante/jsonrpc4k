package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeoutOrNull
import org.khronos.webgl.Uint8Array
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val TERMINATE_TIMEOUT: Duration = 1.seconds
private const val SIGNAL_EXIT_BASE = 128

/** External declarations for the Node.js child_process module. */
@JsModule("child_process")
@JsNonModule
internal external object ChildProcessModule {
    fun spawn(
        command: String,
        args: Array<String>,
        options: dynamic,
    ): dynamic
}

/** External declarations for the Node.js os module. */
@JsModule("os")
@JsNonModule
internal external object OsModule {
    val constants: dynamic
}

internal actual suspend fun startProcessTransport(
    command: List<String>,
    environment: Map<String, String?>,
    workingDirectory: String?,
    stderr: StderrMode,
    exitTimeout: Duration,
): ProcessTransport {
    if (js("typeof process === 'undefined' || !process.versions || !process.versions.node") as Boolean) {
        throw UnsupportedTransportException("Process transports require Node.js; they are not available in a browser")
    }
    val options: dynamic = js("{}")
    if (workingDirectory != null) options.cwd = workingDirectory
    if (environment.isNotEmpty()) options.env = childEnvironment(environment)
    options.stdio =
        arrayOf(
            "pipe",
            "pipe",
            when (stderr) {
                StderrMode.Inherit -> "inherit"
                StderrMode.Discard -> "ignore"
                is StderrMode.Pipe -> "pipe"
            },
        )
    val child = spawnChild(command, options)
    if (stderr is StderrMode.Pipe) drainStderr(child.stderr, stderr.onData)
    return NodeProcessTransport(child, exitTimeout)
}

private fun childEnvironment(changes: Map<String, String?>): dynamic {
    val env: dynamic = js("Object").assign(js("{}"), NodeProcess.env)
    for ((name, value) in changes) {
        // Node skips undefined values, which removes the variable from the child's environment.
        env[name] = value ?: undefined
    }
    return env
}

/** Spawns the child and suspends until Node reports that it started (`spawn`) or failed to (`error`). */
private suspend fun spawnChild(
    command: List<String>,
    options: dynamic,
): dynamic =
    suspendCancellableCoroutine<dynamic> { continuation ->
        val child =
            try {
                ChildProcessModule.spawn(command.first(), command.drop(1).toTypedArray(), options)
            } catch (e: Throwable) {
                continuation.resumeWithException(TransportException("Failed to start process ${command.first()}", e))
                return@suspendCancellableCoroutine
            }
        child.once("error") { error: dynamic ->
            if (continuation.isActive) {
                continuation.resumeWithException(
                    TransportException("Failed to start process ${command.first()}: ${error?.message ?: error}"),
                )
            }
        }
        child.once("spawn") { _: dynamic ->
            if (continuation.isActive) continuation.resume(child)
        }
        continuation.invokeOnCancellation { child.kill("SIGKILL") }
    }

private fun drainStderr(
    stream: dynamic,
    onData: (ByteArray) -> Unit,
) {
    stream.on("data") { chunk: dynamic ->
        try {
            onData(bufferToByteArray(chunk.unsafeCast<Uint8Array>()))
        } catch (ignored: Throwable) {
            // A failing callback must not stop draining, or the child could block on a full pipe.
        }
    }
    stream.on("error") { _: dynamic -> }
}

/** Node.js [ProcessTransport] over `child_process.spawn` with piped stdin and stdout. */
internal class NodeProcessTransport(
    private val child: dynamic,
    private val exitTimeout: Duration,
) : ProcessTransport {
    private var closing = false
    private var ended = false
    private val exit = CompletableDeferred<Int>()
    private val closed = CompletableDeferred<Unit>()
    private val stdin: dynamic = child.stdin
    private val stdout: dynamic = child.stdout

    init {
        child.on("exit") { code: dynamic, signal: dynamic ->
            exit.complete(if (code != null) (code as Int) else SIGNAL_EXIT_BASE + signalNumber(signal as String))
        }
        // Without listeners, a failed kill or write would be an uncaught exception.
        child.on("error") { _: dynamic -> }
        stdin.on("error") { _: dynamic -> }
    }

    override val pid: Long = (child.pid as Int).toLong()

    override val incoming: Flow<ByteArray> =
        callbackFlow {
            val dataCallback: (dynamic) -> Unit = { data: dynamic ->
                if (!closing) trySend(bufferToByteArray(data.unsafeCast<Uint8Array>()))
            }
            val endCallback: (dynamic) -> Unit = {
                ended = true
                close()
            }
            val errorCallback: (dynamic) -> Unit = { error: dynamic ->
                ended = true
                close(if (closing) null else TransportException("Process stdout error: ${error?.message ?: error}"))
            }
            stdout.on("data", dataCallback)
            stdout.once("end", endCallback)
            stdout.once("close", endCallback)
            stdout.once("error", errorCallback)
            awaitClose {
                stdout.removeListener("data", dataCallback)
                stdout.removeListener("end", endCallback)
                stdout.removeListener("close", endCallback)
                stdout.removeListener("error", errorCallback)
            }
        }.buffer(Channel.UNLIMITED)

    /** False after EOF, after [close], or once the child has exited. */
    override val isConnected: Boolean
        get() = !closing && !ended && !exit.isCompleted

    override suspend fun send(data: ByteArray) {
        if (closing || stdin.destroyed as Boolean || stdin.writableEnded as Boolean) {
            throw TransportException("Transport is closed")
        }
        suspendCoroutine { continuation ->
            stdin.write(byteArrayToBuffer(data)) { error: dynamic ->
                if (error != null && error != undefined) {
                    continuation.resumeWithException(TransportException("Write failed: ${error.message ?: error}"))
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    override suspend fun awaitExit(): Int = exit.await()

    override suspend fun close() {
        if (closing) return closed.await()
        closing = true
        stdin.end()
        val failure = terminate()
        // Releases stdout without waiting for a grandchild that still holds it; this ends incoming quietly.
        stdout.destroy()
        if (failure == null) closed.complete(Unit) else closed.completeExceptionally(failure)
        closed.await()
    }

    /** Ends the child after its stdin was closed: exit timeout, then `SIGTERM`, then `SIGKILL`. */
    private suspend fun terminate(): Throwable? {
        if (exitsWithin(exitTimeout)) return null
        child.kill("SIGTERM")
        if (exitsWithin(TERMINATE_TIMEOUT)) return null
        child.kill("SIGKILL")
        return if (exitsWithin(TERMINATE_TIMEOUT)) null else TransportException("Process $pid did not exit after it was killed")
    }

    private suspend fun exitsWithin(timeout: Duration): Boolean = withTimeoutOrNull(timeout) { exit.join() } != null
}

private fun signalNumber(name: String): Int = (OsModule.constants.signals[name] as? Int) ?: 0
