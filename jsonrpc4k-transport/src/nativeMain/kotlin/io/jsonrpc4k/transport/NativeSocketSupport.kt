package io.jsonrpc4k.transport

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.withContext
import kotlin.coroutines.CoroutineContext

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

/**
 * Runs the blocking [open] (an accept or a connect) on [Dispatchers.IO] and hands its transport to the caller.
 *
 * [open] receives the context of the IO coroutine, so a polling loop can notice cancellation. When the caller is
 * cancelled while [open] runs, `withContext` discards the transport that [open] returns and throws
 * [CancellationException] instead; the transport is then closed here, so a socket accepted or connected at that
 * moment does not leak.
 */
internal suspend fun <T : Transport> openOnIo(open: (CoroutineContext) -> T): T {
    var opened: T? = null
    try {
        return withContext(Dispatchers.IO) { open(coroutineContext).also { opened = it } }
    } catch (e: CancellationException) {
        opened?.let { withContext(NonCancellable) { it.close() } }
        throw e
    }
}
