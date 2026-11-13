package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.set

// sockaddr_un.sun_path is 108 bytes on Linux.
internal actual val SUN_PATH_SIZE: Int = 108

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
