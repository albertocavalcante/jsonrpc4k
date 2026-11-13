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
import kotlinx.cinterop.usePinned
import kotlinx.cinterop.value
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.EAGAIN
import platform.posix.EINTR
import platform.posix.FD_CLOEXEC
import platform.posix.F_SETFD
import platform.posix.F_SETFL
import platform.posix.O_NONBLOCK
import platform.posix.O_RDONLY
import platform.posix.O_WRONLY
import platform.posix.POLLIN
import platform.posix.POLLOUT
import platform.posix.SIGKILL
import platform.posix.SIGPIPE
import platform.posix.SIGTERM
import platform.posix.SIG_DFL
import platform.posix._exit
import platform.posix.close
import platform.posix.dup2
import platform.posix.errno
import platform.posix.execvp
import platform.posix.fchdir
import platform.posix.fcntl
import platform.posix.fork
import platform.posix.kill
import platform.posix.pipe
import platform.posix.pollfd
import platform.posix.read
import platform.posix.setenv
import platform.posix.signal
import platform.posix.unsetenv
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
private val NO_DATA = ByteArray(0)

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
            PosixProcessTransport.spawn(command, environment, workingDirectory, stderr, exitTimeout)
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
                    directory = if (workingDirectory != null) newFile(workingDirectory, O_RDONLY) else -1,
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
 * Native [ProcessTransport] for Linux and macOS: `fork` and `execvp` with pipes, a `waitpid` reaper on its own
 * thread, and every wait polled in short slices so that [close] never depends on a thread blocked in a system call.
 *
 * The child inherits this process's environment (plus the requested changes) and descriptors other than the
 * transport's pipes that are not close-on-exec. SIGPIPE is ignored in this process so a write to an exited child
 * fails with `EPIPE`; the child gets the default disposition back. A missing program or working directory makes
 * [ProcessTransport.start] fail: the child reports an `exec` error through a close-on-exec pipe.
 *
 * Between `fork` and `exec` the child runs Kotlin code in a copy of a multithreaded process, so that path is kept to
 * system calls (and, only when the environment is changed, `setenv` string conversions).
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
        scope.launch { exit.complete(waitForExit(pid.toInt())) }
    }

    override val incoming: Flow<ByteArray> =
        flow {
            memScoped {
                val buffer = allocArray<ByteVar>(BUFFER_SIZE)
                while (true) {
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
        if (!pollFd(stdoutFd, POLLIN.convert(), POLL_MILLIS)) return NO_DATA
        val count = read(stdoutFd, buffer, BUFFER_SIZE.convert())
        return when {
            count > 0 -> ByteArray(count.toInt()) { buffer[it] }
            count == 0L -> {
                eof.value = 1
                null
            }
            else -> {
                val err = errno
                if (err == EINTR || err == EAGAIN) return NO_DATA
                if (closing.value != 0) return null
                eof.value = 1
                failTransport("Read failed: ${posixErrorMessage(err)}")
            }
        }
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
        kill(pid.toInt(), SIGTERM)
        if (exitsWithin(TERMINATE_TIMEOUT)) return null
        kill(pid.toInt(), SIGKILL)
        return if (exitsWithin(TERMINATE_TIMEOUT)) null else TransportException("Process $pid did not exit after it was killed")
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
                while (true) {
                    if (!pollFd(fd, POLLIN.convert(), POLL_MILLIS)) {
                        if (exit.isCompleted) break else continue
                    }
                    val count = read(fd, buffer, BUFFER_SIZE.convert())
                    if (count > 0) {
                        deliver(onData, ByteArray(count.toInt()) { buffer[it] })
                    } else if (count == 0L || (errno != EINTR && errno != EAGAIN)) {
                        break
                    }
                }
            }
            close(fd)
        }
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
            val fds = ChildFds.create(stderr, workingDirectory)
            val pid = forkAndExec(command, environment, fds)
            fds.closeChildSide()
            // A negative pid is -errno from fork; otherwise the child reports an exec failure through the pipe.
            val reason = if (pid < 0) -pid else readExecError(fds.exec.readEnd)
            if (reason != 0) {
                fds.closeParentSide()
                if (pid > 0) waitForExit(pid)
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

/**
 * Forks and executes [command]; returns the child's pid in the parent, or -errno when `fork` fails.
 *
 * Everything the child needs is prepared before `fork`, and the child only makes system calls and then `execvp`
 * or `_exit`, so it allocates no Kotlin objects while the other threads of this process do not exist in it.
 */
@OptIn(ExperimentalForeignApi::class)
private fun forkAndExec(
    command: List<String>,
    environment: Map<String, String?>,
    fds: ChildFds,
): Int =
    memScoped {
        val argv = allocArray<CPointerVar<ByteVar>>(command.size + 1)
        command.forEachIndexed { index, argument -> argv[index] = argument.cstr.getPointer(this) }
        argv[command.size] = null
        val names = environment.keys.toList()
        val values = environment.values.toList()
        val errnoBox = alloc<IntVar>()
        val pid = fork()
        if (pid == 0) runChild(command.first(), argv, names, values, fds, errnoBox.ptr)
        if (pid < 0) -errno else pid
    }

/** Runs in the forked child: sets up descriptors and environment, then replaces the process. Never returns. */
@OptIn(ExperimentalForeignApi::class)
private fun runChild(
    program: String,
    argv: CPointer<CPointerVar<ByteVar>>,
    names: List<String>,
    values: List<String?>,
    fds: ChildFds,
    errnoBox: CPointer<IntVar>,
): Nothing {
    dup2(fds.stdin.readEnd, STDIN_FD)
    dup2(fds.stdout.writeEnd, STDOUT_FD)
    if (fds.stderr != null) dup2(fds.stderr.writeEnd, STDERR_FD) else if (fds.devNull >= 0) dup2(fds.devNull, STDERR_FD)
    signal(SIGPIPE, SIG_DFL)
    for (index in 0 until names.size) {
        val value = values[index]
        if (value == null) unsetenv(names[index]) else setenv(names[index], value, 1)
    }
    if (fds.directory >= 0) fchdir(fds.directory)
    execvp(program, argv)
    errnoBox.pointed.value = errno
    write(fds.exec.writeEnd, errnoBox, ERRNO_BYTES.convert())
    while (true) _exit(EXEC_FAILED_STATUS)
}

/** Reads the errno the child reported through the close-on-exec pipe; 0 when `exec` succeeded (EOF). */
@OptIn(ExperimentalForeignApi::class)
private fun readExecError(fd: Int): Int =
    memScoped {
        val box = alloc<IntVar>()
        var count: Long
        do {
            count = read(fd, box.ptr, ERRNO_BYTES.convert())
        } while (count < 0 && errno == EINTR)
        if (count == ERRNO_BYTES.toLong()) box.value else 0
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

/** Waits up to [timeoutMillis] for [events] on [fd]; true when ready (readable, writable or hung up). */
@OptIn(ExperimentalForeignApi::class)
private fun pollFd(
    fd: Int,
    events: Int,
    timeoutMillis: Int,
): Boolean =
    memScoped {
        val entry = alloc<pollfd>()
        entry.fd = fd
        entry.events = events.convert()
        entry.revents = 0
        pollOne(entry.ptr, timeoutMillis) > 0
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
