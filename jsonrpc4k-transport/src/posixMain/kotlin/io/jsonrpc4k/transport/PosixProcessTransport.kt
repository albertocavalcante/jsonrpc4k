package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.IntVar
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.alloc
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.cstr
import kotlinx.cinterop.get
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.set
import kotlinx.cinterop.toKString
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.ENOENT
import platform.posix.ETIMEDOUT
import platform.posix.FD_CLOEXEC
import platform.posix.F_SETFD
import platform.posix.F_SETFL
import platform.posix.O_DIRECTORY
import platform.posix.O_NONBLOCK
import platform.posix.O_RDONLY
import platform.posix.O_WRONLY
import platform.posix.POLLIN
import platform.posix.POLLOUT
import platform.posix.SIGKILL
import platform.posix.SIGPIPE
import platform.posix.SIGTERM
import platform.posix.SIG_DFL
import platform.posix.X_OK
import platform.posix._exit
import platform.posix.access
import platform.posix.close
import platform.posix.dup2
import platform.posix.errno
import platform.posix.execv
import platform.posix.fchdir
import platform.posix.fcntl
import platform.posix.fork
import platform.posix.getenv
import platform.posix.kill
import platform.posix.pipe
import platform.posix.pollfd
import platform.posix.read
import platform.posix.signal
import platform.posix.waitpid
import platform.posix.write
import kotlin.concurrent.AtomicInt
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

private val TERMINATE_TIMEOUT: Duration = 1.seconds
private const val POLL_MILLIS = 100
private const val BUFFER_SIZE = 8192
private const val SIGNAL_EXIT_BASE = 128
private const val SIGNAL_MASK = 0x7f
private const val EXIT_MASK = 0xff
private const val EXIT_SHIFT = 8
private const val EXEC_FAILED_STATUS = 127
private const val STDIN_FD = 0
private const val STDOUT_FD = 1
private const val STDERR_FD = 2
private const val ERRNO_BYTES = 4
private const val ENV_PROGRAM = "/usr/bin/env"
private const val DEFAULT_PATH = "/usr/bin:/bin"
private val EXEC_REPORT_TIMEOUT: Duration = 10.seconds
private val NO_DATA = ByteArray(0)

// Serializes the pipe-then-FD_CLOEXEC sequence against another spawn, so a descriptor is never inherited by a sibling.
private val spawnLock = Mutex()

private fun failTransport(
    message: String,
    cause: Throwable? = null,
): Nothing = throw TransportException(message, cause)

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun startProcessTransport(
    command: List<String>,
    environment: Map<String, String?>,
    workingDirectory: String?,
    stderr: StderrMode,
    exitTimeout: Duration,
): ProcessTransport {
    val context = currentCoroutineContext()
    context.ensureActive()
    // Not cancellable: once started, the child is owned, so a caller cancelled meanwhile still ends it below.
    val transport =
        withContext(NonCancellable + Dispatchers.IO) {
            spawnLock.withLock {
                PosixProcessTransport.spawn(command, environment, workingDirectory, stderr, exitTimeout)
            }
        }
    if (!context.isActive) {
        transport.close()
        context.ensureActive()
    }
    return transport
}

/** The two ends of a pipe; both are close-on-exec until the child's copy is `dup2`ed onto a standard descriptor. */
@OptIn(ExperimentalForeignApi::class)
private class FdPipe(
    val readEnd: Int,
    val writeEnd: Int,
) {
    fun closeBoth() {
        close(readEnd)
        close(writeEnd)
    }

    companion object {
        fun create(): FdPipe =
            memScoped {
                val fds = allocArray<IntVar>(2)
                if (pipe(fds) != 0) failTransport("Failed to create pipe: ${posixErrorMessage(errno)}")
                fcntl(fds[0], F_SETFD, FD_CLOEXEC)
                fcntl(fds[1], F_SETFD, FD_CLOEXEC)
                FdPipe(fds[0], fds[1])
            }
    }
}

/** Descriptors the child needs: the pipes to its standard streams, an optional `/dev/null` and working directory. */
@OptIn(ExperimentalForeignApi::class)
private class ChildFds(
    val stdin: FdPipe,
    val stdout: FdPipe,
    val stderr: FdPipe?,
    val exec: FdPipe,
    val devNull: Int,
    val directory: Int,
) {
    fun closeChildSide() {
        close(stdin.readEnd)
        close(stdout.writeEnd)
        stderr?.let { close(it.writeEnd) }
        close(exec.writeEnd)
        if (devNull >= 0) close(devNull)
        if (directory >= 0) close(directory)
    }

    fun closeParentSide() {
        close(stdin.writeEnd)
        close(stdout.readEnd)
        stderr?.let { close(it.readEnd) }
        close(exec.readEnd)
    }

    companion object {
        fun create(
            stderr: StderrMode,
            workingDirectory: String?,
        ): ChildFds {
            val opened = mutableListOf<Int>()

            fun newPipe() =
                FdPipe.create().also {
                    opened += it.readEnd
                    opened += it.writeEnd
                }

            fun newFile(
                path: String,
                flags: Int,
            ) = openCloexec(path, flags).also { opened += it }
            try {
                return ChildFds(
                    stdin = newPipe(),
                    stdout = newPipe(),
                    stderr = if (stderr is StderrMode.Pipe) newPipe() else null,
                    exec = newPipe(),
                    devNull = if (stderr == StderrMode.Discard) newFile("/dev/null", O_WRONLY) else -1,
                    directory = if (workingDirectory != null) newFile(workingDirectory, O_RDONLY or O_DIRECTORY) else -1,
                )
            } catch (e: TransportException) {
                opened.forEach { close(it) }
                throw e
            }
        }

        private fun openCloexec(
            path: String,
            flags: Int,
        ): Int {
            val fd = platform.posix.open(path, flags)
            if (fd < 0) failTransport("Failed to open $path: ${posixErrorMessage(errno)}")
            fcntl(fd, F_SETFD, FD_CLOEXEC)
            return fd
        }
    }
}

/**
 * Native [ProcessTransport] for Linux and macOS: `fork` and `execv` with pipes, a `waitpid` reaper on a dedicated
 * thread, and every other wait polled in short slices so that [close] never depends on a thread blocked in a system
 * call.
 *
 * The child inherits this process's environment (plus the requested changes) and descriptors other than the
 * transport's pipes that are not close-on-exec. SIGPIPE is ignored in this process so a write to an exited child
 * fails with `EPIPE`; the child gets the default disposition back. A missing program or an unusable working
 * directory makes [ProcessTransport.start] fail: the parent resolves the program on `PATH` (the changed `PATH`, when
 * the environment sets one) and the child reports an `exec` or `fchdir` error through a close-on-exec pipe, which
 * [ProcessTransport.start] waits for at most ten seconds before killing the child.
 *
 * A changed environment is applied by executing through `/usr/bin/env`, so the child never touches the environment.
 * Between `fork` and `exec` the child makes only system calls on memory prepared before the fork (the `execv` path
 * string conversion is the one small allocation Kotlin/Native imposes), because only the forking thread exists in it;
 * should it still stall, the start timeout above kills it. Spawns are serialized so that a pipe is not inherited by a concurrent spawn; descriptors
 * created elsewhere in the process are not covered by that lock.
 */
@OptIn(ExperimentalForeignApi::class)
internal class PosixProcessTransport private constructor(
    override val pid: Long,
    private val stdinFd: Int,
    private val stdoutFd: Int,
    private val exitTimeout: Duration,
) : ProcessTransport {
    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())
    private val closing = AtomicInt(0)
    private val eof = AtomicInt(0)
    private val readLock = Mutex()
    private val writeLock = Mutex()
    private val exit = CompletableDeferred<Int>()
    private val closed = CompletableDeferred<Unit>()

    init {
        reapOnOwnThread()
    }

    /** Waits for the child on a dedicated thread, so a blocked `waitpid` never occupies the shared IO pool. */
    @OptIn(DelicateCoroutinesApi::class)
    private fun reapOnOwnThread() {
        val reaper = newSingleThreadContext("jsonrpc4k-reaper-$pid")
        scope.launch {
            try {
                exit.complete(withContext(reaper) { waitForExit(pid.toInt()) })
            } finally {
                reaper.close()
            }
        }
    }

    override val incoming: Flow<ByteArray> =
        flow {
            memScoped {
                val buffer = allocArray<ByteVar>(BUFFER_SIZE)
                while (true) {
                    // Idle slices never suspend, so a cancelled collector (e.g. `first`) must be noticed here.
                    currentCoroutineContext().ensureActive()
                    val chunk = readLock.withLock { readStep(buffer) } ?: break
                    if (chunk.isNotEmpty()) emit(chunk)
                }
            }
        }.flowOn(Dispatchers.IO)

    /** False after EOF, after [close], or once the child has exited. */
    override val isConnected: Boolean
        get() = closing.value == 0 && eof.value == 0 && !exit.isCompleted

    /** Returns the next chunk, [NO_DATA] after a poll slice without data, or null at EOF or after close. */
    private fun readStep(buffer: CPointer<ByteVar>): ByteArray? {
        if (closing.value != 0) return null
        val readable =
            try {
                pollFd(stdoutFd, POLLIN.convert(), POLL_MILLIS)
            } catch (e: PollFailedException) {
                return failRead(e)
            }
        if (!readable) return NO_DATA
        val count = read(stdoutFd, buffer, BUFFER_SIZE.convert())
        return when {
            count > 0 -> ByteArray(count.toInt()) { buffer[it] }
            count == 0L -> {
                eof.value = 1
                null
            }
            else -> {
                val err = errno
                if (err == EINTR || err == EAGAIN) NO_DATA else failRead(TransportException("Read failed: ${posixErrorMessage(err)}"))
            }
        }
    }

    /** Ends incoming: quietly once closing, otherwise by marking EOF and throwing [failure]. */
    private fun failRead(failure: TransportException): ByteArray? {
        if (closing.value != 0) return null
        eof.value = 1
        throw failure
    }

    override suspend fun send(data: ByteArray): Unit =
        withContext(Dispatchers.IO) {
            writeLock.withLock {
                data.usePinned { pinned ->
                    var offset = 0
                    while (offset < data.size) {
                        if (closing.value != 0) failTransport("Transport is closed")
                        offset += writeSome(pinned.addressOf(offset), data.size - offset)
                    }
                }
            }
        }

    /** Writes at most [length] bytes, waiting a poll slice when the pipe is full; returns the bytes written. */
    private fun writeSome(
        start: CPointer<ByteVar>,
        length: Int,
    ): Int {
        val count = write(stdinFd, start, length.convert())
        if (count >= 0) return count.toInt()
        val err = errno
        if (err == EAGAIN) {
            pollFd(stdinFd, POLLOUT.convert(), POLL_MILLIS)
        } else if (err != EINTR) {
            failTransport("Write failed: ${posixErrorMessage(err)}")
        }
        return 0
    }

    override suspend fun awaitExit(): Int = exit.await()

    override suspend fun close() {
        if (!closing.compareAndSet(0, 1)) return closed.await()
        val failure =
            withContext(NonCancellable + Dispatchers.IO) {
                // Waiters notice `closing` within a poll slice, so these locks are never held for long.
                writeLock.withLock { close(stdinFd) }
                val terminated = terminate()
                readLock.withLock { close(stdoutFd) }
                terminated
            }
        if (failure == null) closed.complete(Unit) else closed.completeExceptionally(failure)
        closed.await()
    }

    /** Ends the child after its stdin was closed: exit timeout, then `SIGTERM`, then `SIGKILL`. */
    private suspend fun terminate(): Throwable? {
        if (exitsWithin(exitTimeout)) return null
        signalIfRunning(SIGTERM)
        if (exitsWithin(TERMINATE_TIMEOUT)) return null
        signalIfRunning(SIGKILL)
        return if (exitsWithin(TERMINATE_TIMEOUT)) null else TransportException("Process $pid did not exit after it was killed")
    }

    /** Signals the child unless it was already reaped, whose pid could since belong to another process. */
    private fun signalIfRunning(signal: Int) {
        if (!exit.isCompleted) kill(pid.toInt(), signal)
    }

    private suspend fun exitsWithin(timeout: Duration): Boolean = withTimeoutOrNull(timeout) { exit.join() } != null

    /** Delivers the child's stderr to [onData] until EOF, or until the child is gone and the pipe is quiet. */
    private fun drainStderr(
        fd: Int,
        onData: (ByteArray) -> Unit,
    ) {
        scope.launch {
            memScoped {
                val buffer = allocArray<ByteVar>(BUFFER_SIZE)
                var pollFailed = false
                while (true) {
                    if (pollFailed) {
                        delay(POLL_MILLIS.toLong())
                    } else {
                        val readable = pollStderr(fd)
                        if (readable == null) {
                            // Retrying poll() would spin, and closing the pipe would make the child's next stderr
                            // write fail with EPIPE or kill it with SIGPIPE. Fall back to non-blocking reads paced
                            // by delay, ten per second, until EOF or until the child is gone and the pipe is quiet.
                            pollFailed = true
                            fcntl(fd, F_SETFL, O_NONBLOCK)
                        } else if (!readable) {
                            if (exit.isCompleted) break else continue
                        }
                    }
                    if (!readStderr(fd, buffer, onData)) break
                }
            }
            close(fd)
        }
    }

    /** Reads once from stderr and delivers what arrived; false at EOF, on a read error, or once quiet after exit. */
    private fun readStderr(
        fd: Int,
        buffer: CPointer<ByteVar>,
        onData: (ByteArray) -> Unit,
    ): Boolean {
        val count = read(fd, buffer, BUFFER_SIZE.convert())
        val err = errno
        return when {
            count > 0 -> {
                deliver(onData, ByteArray(count.toInt()) { buffer[it] })
                true
            }
            count == 0L -> false
            err == EAGAIN -> !exit.isCompleted
            else -> err == EINTR
        }
    }

    /** Waits one slice for stderr data: true when readable or hung up, false when quiet, null when poll() failed. */
    private fun pollStderr(fd: Int): Boolean? =
        try {
            pollFd(fd, POLLIN.convert(), POLL_MILLIS)
        } catch (ignored: PollFailedException) {
            null
        }

    companion object {
        /** Starts the child. Runs blocking system calls; call it on [Dispatchers.IO]. */
        fun spawn(
            command: List<String>,
            environment: Map<String, String?>,
            workingDirectory: String?,
            stderr: StderrMode,
            exitTimeout: Duration,
        ): PosixProcessTransport {
            // A peer that closed its end must surface as EPIPE, not kill the process.
            ignoreSigpipeOnce()
            val plan = ExecPlan.create(command, environment)
            val fds = ChildFds.create(stderr, workingDirectory)
            val pid = forkAndExec(plan, fds)
            fds.closeChildSide()
            // A negative pid is -errno from fork; otherwise the child reports a setup failure through the pipe.
            val reason = if (pid < 0) -pid else readExecError(fds.exec.readEnd)
            if (reason != 0) {
                fds.closeParentSide()
                if (pid > 0) {
                    // The child may still be running: it may have exec'd while the report timed out or poll or read
                    // failed. It has not been reaped yet, so its pid cannot belong to another process.
                    kill(pid, SIGKILL)
                    waitForExit(pid)
                }
                failTransport("Failed to start process ${command.first()}: ${posixErrorMessage(reason)}")
            }
            close(fds.exec.readEnd)
            fcntl(fds.stdin.writeEnd, F_SETFL, O_NONBLOCK)
            val transport = PosixProcessTransport(pid.toLong(), fds.stdin.writeEnd, fds.stdout.readEnd, exitTimeout)
            if (stderr is StderrMode.Pipe && fds.stderr != null) transport.drainStderr(fds.stderr.readEnd, stderr.onData)
            return transport
        }
    }
}

/** What the child executes: the resolved program path and its argument vector, with `env` prepended for changes. */
private class ExecPlan(
    val program: String,
    val argv: List<String>,
) {
    companion object {
        @OptIn(ExperimentalForeignApi::class)
        fun create(
            command: List<String>,
            environment: Map<String, String?>,
        ): ExecPlan {
            require(command.isNotEmpty()) { "command must not be empty" }
            environment.keys.forEach {
                val valid = it.isNotEmpty() && '=' !in it && '\u0000' !in it && !it.startsWith("-")
                require(valid) { "Invalid environment variable name: $it" }
            }
            val path = environment["PATH"] ?: if ("PATH" in environment) null else getenv("PATH")?.toKString()
            val program = resolveProgram(command.first(), path ?: DEFAULT_PATH)
            if (environment.isEmpty()) return ExecPlan(program, command)
            val unset = environment.filterValues { it == null }.keys.flatMap { listOf("-u", it) }
            val assignments = environment.mapNotNull { (name, value) -> value?.let { "$name=$it" } }
            return ExecPlan(ENV_PROGRAM, listOf(ENV_PROGRAM) + unset + assignments + program + command.drop(1))
        }

        /** Finds [name] on [path] like `execvp` does; a name with a slash is used as given. */
        private fun resolveProgram(
            name: String,
            path: String,
        ): String {
            if ('/' in name) return name
            val found = path.split(':').map { "${it.ifEmpty { "." }}/$name" }.firstOrNull { access(it, X_OK) == 0 }
            return found ?: failTransport("Failed to start process $name: ${posixErrorMessage(ENOENT)}")
        }
    }
}

/**
 * Forks and executes [plan]; returns the child's pid in the parent, or -errno when `fork` fails.
 *
 * Everything the child needs is prepared before `fork`, and the child only makes system calls and then `execv`
 * or `_exit`, so it allocates no Kotlin objects while the other threads of this process do not exist in it.
 */
@OptIn(ExperimentalForeignApi::class)
private fun forkAndExec(
    plan: ExecPlan,
    fds: ChildFds,
): Int =
    memScoped {
        val argv = allocArray<CPointerVar<ByteVar>>(plan.argv.size + 1)
        plan.argv.forEachIndexed { index, argument -> argv[index] = argument.cstr.getPointer(this) }
        argv[plan.argv.size] = null
        val errnoBox = alloc<IntVar>()
        val pid = fork()
        if (pid == 0) runChild(plan.program, argv, fds, errnoBox.ptr)
        if (pid < 0) -errno else pid
    }

/** Runs in the forked child: sets up descriptors, then replaces the process. Never returns. */
@OptIn(ExperimentalForeignApi::class)
private fun runChild(
    program: String,
    argv: CPointer<CPointerVar<ByteVar>>,
    fds: ChildFds,
    errnoBox: CPointer<IntVar>,
): Nothing {
    dup2(fds.stdin.readEnd, STDIN_FD)
    dup2(fds.stdout.writeEnd, STDOUT_FD)
    if (fds.stderr != null) {
        dup2(fds.stderr.writeEnd, STDERR_FD)
    } else if (fds.devNull >= 0) {
        dup2(fds.devNull, STDERR_FD)
    }
    signal(SIGPIPE, SIG_DFL)
    if (fds.directory >= 0 && fchdir(fds.directory) != 0) reportAndExit(fds, errnoBox)
    execv(program, argv)
    reportAndExit(fds, errnoBox)
}

/** Writes the current errno to the exec pipe and ends the child. */
@OptIn(ExperimentalForeignApi::class)
private fun reportAndExit(
    fds: ChildFds,
    errnoBox: CPointer<IntVar>,
): Nothing {
    errnoBox.pointed.value = errno
    write(fds.exec.writeEnd, errnoBox, ERRNO_BYTES.convert())
    while (true) _exit(EXEC_FAILED_STATUS)
}

/**
 * Reads the errno the child reported through the close-on-exec pipe: 0 when `exec` succeeded (EOF), [ETIMEDOUT] when
 * the child reported nothing within [EXEC_REPORT_TIMEOUT], or the errno of a failed read.
 */
@OptIn(ExperimentalForeignApi::class)
private fun readExecError(fd: Int): Int =
    memScoped {
        val deadlineSlices = EXEC_REPORT_TIMEOUT.inWholeMilliseconds / POLL_MILLIS
        var slices = 0L
        try {
            while (!pollFd(fd, POLLIN.convert(), POLL_MILLIS)) {
                if (++slices >= deadlineSlices) return@memScoped ETIMEDOUT
            }
        } catch (e: PollFailedException) {
            return@memScoped e.errno
        }
        val box = alloc<IntVar>()
        var count: Long
        do {
            count = read(fd, box.ptr, ERRNO_BYTES.convert())
        } while (count < 0 && errno == EINTR)
        when {
            count == ERRNO_BYTES.toLong() -> box.value
            count < 0 -> errno
            else -> 0
        }
    }

/** Blocks until [pid] exits and returns its exit code, or 128 plus the signal number when a signal ended it. */
@OptIn(ExperimentalForeignApi::class)
private fun waitForExit(pid: Int): Int =
    memScoped {
        val status = alloc<IntVar>()
        var result: Int
        do {
            result = waitpid(pid, status.ptr, 0)
        } while (result < 0 && errno == EINTR)
        if (result == pid) decodeStatus(status.value) else -1
    }

private fun decodeStatus(status: Int): Int {
    val signal = status and SIGNAL_MASK
    return if (signal == 0) (status shr EXIT_SHIFT) and EXIT_MASK else SIGNAL_EXIT_BASE + signal
}

/** A `poll()` failure other than `EINTR`; [errno] is its reason. */
internal class PollFailedException(
    val errno: Int,
) : TransportException("Poll failed: ${posixErrorMessage(errno)}")

/**
 * Waits up to [timeoutMillis] for [events] on [fd]; true when ready (readable, writable or hung up), false on timeout
 * or when a signal interrupted the wait (`EINTR`), so the caller retries. Any other failure throws
 * [PollFailedException]: it does not go away on retry, and retrying would spin. [poll] is the system call, replaced
 * only by tests.
 */
@OptIn(ExperimentalForeignApi::class)
internal fun pollFd(
    fd: Int,
    events: Int,
    timeoutMillis: Int,
    poll: (CPointer<pollfd>, Int) -> Int = ::pollOne,
): Boolean =
    memScoped {
        val entry = alloc<pollfd>()
        entry.fd = fd
        entry.events = events.convert()
        entry.revents = 0
        val ready = poll(entry.ptr, timeoutMillis)
        if (ready < 0) {
            val err = errno
            if (err != EINTR) throw PollFailedException(err)
        }
        ready > 0
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
