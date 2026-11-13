package io.jsonrpc4k.transport

import kotlinx.cinterop.ByteVar
import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.set

// sockaddr_un.sun_path is 104 bytes on Apple platforms.
internal actual val SUN_PATH_SIZE: Int = 104

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
