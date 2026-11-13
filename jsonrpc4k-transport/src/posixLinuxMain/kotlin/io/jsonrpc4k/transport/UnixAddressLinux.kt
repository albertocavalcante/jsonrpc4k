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

// sockaddr_un.sun_path is 108 bytes on Linux.
private const val LINUX_SUN_PATH_BYTES = 108

internal actual val SUN_PATH_SIZE: Int = LINUX_SUN_PATH_BYTES

private const val AF_UNIX_VALUE: Byte = 1

/** Linux layout: a little-endian 16 bit sa_family at offset 0 (every Linux target is little-endian). */
@OptIn(ExperimentalForeignApi::class)
internal actual fun writeUnixFamily(
    buffer: CPointer<ByteVar>,
    length: Int,
) {
    buffer[0] = AF_UNIX_VALUE
    buffer[1] = 0
}

@OptIn(ExperimentalForeignApi::class)
internal actual fun lstatMode(path: String): Int =
    memScoped {
        val info = alloc<stat>()
        if (lstat(path, info.ptr) == 0) info.st_mode.toInt() else -1
    }
