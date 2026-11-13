package io.jsonrpc4k.transport

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val DEFAULT_EXIT_TIMEOUT: Duration = 2.seconds

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
     * This library drains the child's standard error until EOF and passes each chunk to [onData], in order. The
     * callback runs on a library-owned thread (JVM, POSIX) or the event loop (Node); do not block in it. Exceptions
     * thrown by [onData] are ignored, so draining never stops.
     */
    public class Pipe(
        public val onData: (ByteArray) -> Unit,
    ) : StderrMode
}

/**
 * [Transport] over the standard input and output of a child process that it starts and owns, such as a BSP server
 * launched from a `.bsp` connection file.
 *
 * [incoming] reads the child's stdout and [send] writes its stdin. Sessions never close transports, so the owner
 * calls [close]. It closes the child's stdin so the child sees EOF, waits up to the exit timeout for it to exit,
 * then terminates it (`SIGTERM` on Unix), then kills it (`SIGKILL`), waiting a bounded time after each step, and
 * finally releases stdout without blocking. [close] therefore returns within the exit timeout plus a few seconds,
 * even when the child, or a grandchild that inherited its pipes, keeps running. Only the child itself is ended:
 * while such a grandchild holds the child's stdout, [incoming] can stay open until the grandchild exits.
 *
 * ## Platform support
 *
 * | Platform | Support |
 * |---|---|
 * | JVM | `ProcessBuilder` |
 * | Node.js | `child_process.spawn` |
 * | Linux, macOS (native) | `fork` and `execvp` with pipes and a `waitpid` reaper |
 * | Windows (mingw), browser | [UnsupportedTransportException] from [start] |
 */
public interface ProcessTransport : Transport {
    /** Operating-system id of the child process. */
    public val pid: Long

    /**
     * Suspends until the child exits and returns its exit code; it does not close the transport. On Unix a child
     * ended by a signal reports 128 plus the signal number, such as 143 after `SIGTERM` and 137 after `SIGKILL`.
     */
    public suspend fun awaitExit(): Int

    public companion object {
        /**
         * Starts [command] with piped stdin and stdout and returns a transport that owns the child.
         *
         * @param command program and arguments; the program is resolved through `PATH` as the platform's process
         *   API does.
         * @param environment changes to the inherited environment: a value sets a variable and `null` removes it.
         * @param workingDirectory directory the child starts in, or `null` for this process's working directory.
         * @param stderr where the child's standard error goes.
         * @param exitTimeout how long [close] waits for the child to exit after closing its stdin before ending
         *   it; [Duration.ZERO] ends it at once.
         * @throws TransportException when the process cannot be started.
         * @throws UnsupportedTransportException on platforms without process support (Windows native, browser).
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
            return startProcessTransport(command, environment, workingDirectory, stderr, exitTimeout)
        }
    }
}

/** Platform start of a child process; arguments are already validated by [ProcessTransport.start]. */
internal expect suspend fun startProcessTransport(
    command: List<String>,
    environment: Map<String, String?>,
    workingDirectory: String?,
    stderr: StderrMode,
    exitTimeout: Duration,
): ProcessTransport
