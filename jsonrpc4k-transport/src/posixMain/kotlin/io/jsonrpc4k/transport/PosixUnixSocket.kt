package io.jsonrpc4k.transport

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.convert
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext
import platform.posix.AF_UNIX
import platform.posix.ECONNREFUSED
import platform.posix.SOCK_STREAM
import platform.posix.bind
import platform.posix.close
import platform.posix.connect
import platform.posix.errno
import platform.posix.listen
import platform.posix.sockaddr
import platform.posix.socket
import platform.posix.unlink

/**
 * Fills a `sockaddr_un` for [path] and passes it with its length to [block]. The struct layout
 * differs between Apple and Linux, so each family builds it in its own source set.
 */
@OptIn(ExperimentalForeignApi::class)
internal expect fun <R> withUnixAddress(
    path: String,
    block: (address: CPointer<sockaddr>, length: Int) -> R,
): R

/** Longest socket path in bytes, excluding the terminating NUL; differs per platform family. */
internal expect val UNIX_PATH_MAX_BYTES: Int

/** Whether [path] exists and is itself a socket (a symbolic link is not followed). */
@OptIn(ExperimentalForeignApi::class)
internal expect fun isSocketFile(path: String): Boolean

private fun checkUnixPath(path: String) {
    if (path.isEmpty() || '\u0000' in path) throw TransportException("Invalid Unix socket path")
    val size = path.encodeToByteArray().size
    if (size > UNIX_PATH_MAX_BYTES) {
        throw TransportException("Unix socket path is $size bytes; this platform allows at most $UNIX_PATH_MAX_BYTES")
    }
}

@OptIn(ExperimentalForeignApi::class)
private fun newUnixSocket(): Int {
    // A peer that closed its end must surface as EPIPE, not kill the process.
    ignoreSigpipeOnce()
    val fd = socket(AF_UNIX, SOCK_STREAM, 0)
    if (fd < 0) throw TransportException("Failed to create Unix socket: ${posixErrorMessage(errno)}")
    return fd
}

/** Connects [fd] to [path]; returns 0 on success or the errno of the failure. */
@OptIn(ExperimentalForeignApi::class)
private fun connectUnix(
    fd: Int,
    path: String,
): Int = withUnixAddress(path) { address, length -> if (connect(fd, address, length.convert()) < 0) errno else 0 }

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun connectUnixSocket(path: String): Transport =
    withContext(Dispatchers.IO) {
        checkUnixPath(path)
        val fd = newUnixSocket()
        val failure = connectUnix(fd, path)
        if (failure != 0) {
            close(fd)
            throw TransportException("Failed to connect to Unix socket $path: ${posixErrorMessage(failure)}")
        }
        PosixSocketTransport(fd)
    }

@OptIn(ExperimentalForeignApi::class)
internal actual suspend fun bindUnixSocket(path: String): LocalListener =
    withContext(Dispatchers.IO) {
        checkUnixPath(path)
        removeStaleSocketFile(path)
        val fd = newUnixSocket()
        val bound = withUnixAddress(path) { address, length -> bind(fd, address, length.convert()) } == 0
        val failure = if (bound && listen(fd, LISTEN_BACKLOG) == 0) 0 else errno
        if (failure != 0) {
            close(fd)
            // Only remove a file this call created.
            if (bound) unlink(path)
            throw TransportException("Failed to listen on Unix socket $path: ${posixErrorMessage(failure)}")
        }
        PosixUnixListener(PosixListener(fd, -1) { unlink(path) }, path)
    }

/**
 * Deletes [path] only when it is a socket that refuses connections. A regular file or a socket that
 * is still served is left alone and makes the following bind fail.
 */
@OptIn(ExperimentalForeignApi::class)
private fun removeStaleSocketFile(path: String) {
    if (!isSocketFile(path)) return
    val probe = newUnixSocket()
    val failure = connectUnix(probe, path)
    close(probe)
    when (failure) {
        0 -> throw TransportException("Unix socket $path is already in use")
        ECONNREFUSED -> unlink(path)
        else -> throw TransportException("Cannot probe Unix socket $path: ${posixErrorMessage(failure)}")
    }
}

/** A [PosixListener] on a socket file; the file is deleted when closing starts. */
internal class PosixUnixListener(
    delegate: TransportListener,
    override val path: String,
) : LocalListener,
    TransportListener by delegate
