package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Asks the peer to cancel the outbound requests that the local side stopped waiting for.
 *
 * A caller stops waiting when it is cancelled or its timeout elapses, so it may be cancelled itself;
 * the `$/cancelRequest` notification is therefore sent from the connection's [scope]. It is an
 * ordinary outgoing message, sent best effort: it is dropped when the connection closes, fails or
 * cannot admit it.
 */
internal class OutboundCancellation(
    private val pending: PendingRequests,
    private val scope: CoroutineScope,
    private val send: suspend (params: JsonElement) -> Unit,
) {
    /**
     * Stops waiting for the sent request [id]. When its response has not arrived, the peer is asked
     * to cancel the request, and a response that arrives later is ignored because no waiter remains.
     */
    fun abandon(
        id: RequestId,
        waiter: CompletableDeferred<JsonElement?>,
    ) {
        if (!pending.cancel(id, waiter)) return
        val params = cancelRequestParams(id)
        scope.launch {
            try {
                send(params)
            } catch (_: Throwable) {
                // The connection closed or failed first, and its state already reports that outcome.
            }
        }
    }
}

/** The `$/cancelRequest` params that name the request [id]. */
internal fun cancelRequestParams(id: RequestId): JsonObject {
    val wireId =
        when (id) {
            is RequestId.NumberId -> JsonPrimitive(id.value)
            is RequestId.StringId -> JsonPrimitive(id.value)
        }
    return JsonObject(mapOf("id" to wireId))
}
