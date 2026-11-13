package io.jsonrpc4k.transport

private const val BYTE_MASK = 0xFF
private const val BYTE_BITS = 8

/**
 * Convert a `sockaddr_in.sin_port` value (network byte order) to a plain port number.
 *
 * Swapping the two bytes is correct because every supported Kotlin/Native target (x64, arm64) is
 * little-endian, and it avoids `ntohs`, which is a macro on some platforms and not exposed by the
 * platform bindings.
 */
internal fun networkToHostPort(networkOrder: UShort): Int {
    val raw = networkOrder.toInt()
    return ((raw and BYTE_MASK) shl BYTE_BITS) or (raw shr BYTE_BITS)
}
