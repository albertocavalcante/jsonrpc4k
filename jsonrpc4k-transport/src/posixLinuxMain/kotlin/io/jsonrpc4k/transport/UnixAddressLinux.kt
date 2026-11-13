package io.jsonrpc4k.transport

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.ptr
import kotlinx.cinterop.reinterpret
import kotlinx.cinterop.set
import kotlinx.cinterop.sizeOf
import platform.posix.AF_UNIX
import platform.posix.lstat
import platform.posix.memset
import platform.posix.sockaddr
import platform.posix.sockaddr_un
import platform.posix.stat

// sun_path is 108 bytes on Linux; one is kept for the NUL.
internal actual val UNIX_PATH_MAX_BYTES: Int = 107

private const val FILE_TYPE_MASK = 0xF000u
private const val FILE_TYPE_SOCKET = 0xC000u

@OptIn(ExperimentalForeignApi::class)
internal actual fun <R> withUnixAddress(
    path: String,
    block: (address: CPointer<sockaddr>, length: Int) -> R,
): R =
    memScoped {
        val address = alloc<sockaddr_un>()
        val length = sizeOf<sockaddr_un>().toInt()
        memset(address.ptr, 0, length.convert())
        address.sun_family = AF_UNIX.convert()
        val bytes = path.encodeToByteArray()
        for (index in bytes.indices) address.sun_path[index] = bytes[index]
        block(address.ptr.reinterpret(), length)
    }

@OptIn(ExperimentalForeignApi::class)
internal actual fun isSocketFile(path: String): Boolean =
    memScoped {
        val info = alloc<stat>()
        lstat(path, info.ptr) == 0 && (info.st_mode.toUInt() and FILE_TYPE_MASK) == FILE_TYPE_SOCKET
    }
