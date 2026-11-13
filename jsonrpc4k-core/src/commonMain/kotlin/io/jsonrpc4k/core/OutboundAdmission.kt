package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.selects.select

/** The token channel is never closed, so a cancelled selected receiver can return its token. */
internal class OutboundAdmission(
    private val ended: CompletableDeferred<Unit>,
    private val fail: (Throwable) -> Unit,
) {
    private val tokens: Channel<Unit>

    init {
        tokens = Channel(1, onUndeliveredElement = { release() })
        check(tokens.trySend(Unit).isSuccess)
    }

    suspend fun acquire() {
        select<Unit> {
            tokens.onReceive { }
            ended.onAwait { error("Outbound terminal signal completed normally") }
        }
    }

    /** Takes the token only when it is free now; a token handed to a waiting producer is never taken. */
    fun tryAcquire(): Boolean = tokens.tryReceive().isSuccess

    fun release() {
        if (!tokens.trySend(Unit).isSuccess) fail(IllegalStateException("Outbound admission token was returned twice"))
    }
}
