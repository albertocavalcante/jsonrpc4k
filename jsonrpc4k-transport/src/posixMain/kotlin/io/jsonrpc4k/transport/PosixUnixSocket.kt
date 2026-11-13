package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.allocArray
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.set
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

/** Bytes of `sun_path` including the terminating NUL; 108 on Linux, 104 on Apple platforms. */
internal expect val SUN_PATH_SIZE: Int

/**
 * Writes the address family (and on Apple the `sun_len` byte) into the first [SUN_PATH_OFFSET]
 * bytes of a zeroed `sockaddr_un` buffer of [length] bytes. The cinterop struct is not visible to
 * the shared intermediate source sets, so the layout is written by hand.
 */
@OptIn(ExperimentalForeignApi::class)
internal expect fun writeUnixFamily(
    buffer: CPointer<ByteVar>,
    length: Int,
)

/**
 * The `st_mode` of [path] from `lstat` (symbolic links are not followed), or -1 when the call fails.
 * `st_mode` is 16 bits wide on Apple and 32 bits on Linux, so the platform source sets widen it.
 */
internal expect fun lstatMode(path: String): Int

/** Offset of `sun_path`; both families put a two byte header before it. */
private const val SUN_PATH_OFFSET = 2

/** Longest socket path in bytes, excluding the terminating NUL; differs per platform family. */
internal val UNIX_PATH_MAX_BYTES: Int
    get() = SUN_PATH_SIZE - 1

/**
 * Builds a `sockaddr_un` for [path] in a zeroed byte buffer and passes it with its length to [block].
 */
@OptIn(ExperimentalForeignApi::class)
internal fun <R> withUnixAddress(
    path: String,
    block: (address: CPointer<sockaddr>, length: Int) -> R,
): R =
    memScoped {
        val length = SUN_PATH_OFFSET + SUN_PATH_SIZE
        val buffer = allocArray<ByteVar>(length)
        val bytes = path.encodeToByteArray()
        for (index in bytes.indices) buffer[SUN_PATH_OFFSET + index] = bytes[index]
        writeUnixFamily(buffer, length)
        block(buffer.reinterpret(), length)
    }

/** Whether [path] exists and is itself a socket (a symbolic link is not followed). */
@OptIn(ExperimentalForeignApi::class)
internal fun isSocketFile(path: String): Boolean {
    val mode = lstatMode(path)
    return mode >= 0 && (mode and FILE_TYPE_MASK) == FILE_TYPE_SOCKET
}

private const val FILE_TYPE_MASK = 0xF000
private const val FILE_TYPE_SOCKET = 0xC000

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
        adoptSocket(fd)
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
