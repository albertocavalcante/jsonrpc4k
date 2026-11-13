package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.set
import platform.posix.lstat
import platform.posix.stat

// sockaddr_un.sun_path is 104 bytes on Apple platforms.
private const val APPLE_SUN_PATH_BYTES = 104

internal actual val SUN_PATH_SIZE: Int = APPLE_SUN_PATH_BYTES

private const val AF_UNIX_VALUE: Byte = 1

/** Apple layout: sun_len at offset 0 followed by the one byte sa_family. */
@OptIn(ExperimentalForeignApi::class)
internal actual fun writeUnixFamily(
    buffer: CPointer<ByteVar>,
    length: Int,
) {
    buffer[0] = length.toByte()
    buffer[1] = AF_UNIX_VALUE
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun lstatMode(path: String): Int =
    memScoped {
        val info = alloc<stat>()
        if (lstat(path, info.ptr) == 0) info.st_mode.toInt() else -1
    }
