package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Asks the peer to cancel the outbound requests that the local side stopped waiting for.
 *
 * The `$/cancelRequest` notification is queued at once, after the request and before anything the
 * caller sends next, and it never waits: [trySend] queues it without suspending and without taking a
 * producer slot, or refuses it. A refused notification is dropped, and so is one that would exceed
 * eight notifications not yet dequeued by the outgoing collector, so that cancellations can neither
 * block a caller nor crowd out the replies that a connection must queue.
 */
internal class OutboundCancellation(
    private val pending: PendingRequests,
    private val trySend: (notification: NotificationMessage, onDequeued: () -> Unit) -> Boolean,
) {
    private val queued = MutableStateFlow(0)

    /**
     * Stops waiting for the sent request [id]. When its response has not arrived, the peer is asked
     * to cancel the request, and a response that arrives later is ignored because no waiter remains.
     */
    fun abandon(
        id: RequestId,
        waiter: CompletableDeferred<JsonElement?>,
    ) {
        if (!pending.cancel(id, waiter) || !claimSlot()) return
        val notification = NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD, cancelRequestParams(id))
        if (!trySend(notification, ::releaseSlot)) releaseSlot()
    }

    private fun claimSlot(): Boolean {
        while (true) {
            val current = queued.value
            if (current >= MAX_QUEUED) return false
            if (queued.compareAndSet(current, current + 1)) return true
        }
    }

    private fun releaseSlot() {
        while (true) {
            val current = queued.value
            if (queued.compareAndSet(current, current - 1)) return
        }
    }

    private companion object {
        const val MAX_QUEUED = 8
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
