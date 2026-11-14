package io.jsonrpc4k.core

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.serialization.json.JsonElement

/**
 * The dispatch state of one peer: its running inbound requests and the waiters of the requests
 * sent to it. Create one per connection with [Dispatcher.newPeer]; the handlers stay in the shared
 * [dispatcher].
 *
 * Request ids are scoped to the peer. An inbound `$/cancelRequest` cancels the running handler of
 * the request with that id from this peer only, never a request of another peer that shares the
 * [dispatcher], and never an outbound waiter registered with the same id. Once the cancelled
 * handler stops, [dispatch] answers the request with a Request Cancelled error (-32800); a handler
 * that throws an exception of its own instead is answered as for that exception, and a value that
 * it returns after the cancellation is discarded. Cancellation is cooperative and reaches a handler
 * only while it runs, so it needs messages to be dispatched concurrently. Each handler runs in its
 * own child job, so its response also waits for coroutines that it starts in that job. An inbound
 * response completes the waiter of this peer that has its id.
 */
public class PeerDispatcher internal constructor(
    /** The shared routing table that this peer's messages are dispatched to. */
    public val dispatcher: Dispatcher,
) {
    internal val pending = PendingRequests()
    internal val inbound = InboundRequests()

    /**
     * Dispatch an incoming message.
     * Returns a response message if the input was a request, null otherwise. A request's handler
     * runs until it returns, or until a `$/cancelRequest` for its id from this peer, dispatched
     * concurrently, cancels it.
     */
    public suspend fun dispatch(message: Message): ResponseMessage? =
        when (message) {
            is RequestMessage -> dispatcher.handleRequest(message, inbound)
            is NotificationMessage -> {
                if (message.method == Dispatcher.CANCEL_REQUEST_METHOD) {
                    // Only inbound handlers are cancelled, so an outbound waiter with the same id keeps waiting.
                    cancelledRequestId(message.params)?.let(inbound::cancel)
                } else {
                    dispatcher.handleNotification(message)
                }
                null
            }
            is ResponseMessage -> {
                pending.complete(message)
                null
            }
        }

    /**
     * Dispatch the entries of an inbound batch in wire order and return the responses owed for it
     * (JSON-RPC 2.0 section 6): one per request, and one Invalid Request error with a null id per
     * invalid entry. Notifications and responses add nothing, so an empty result means that no reply
     * is sent, as for a batch of only notifications. Entries run sequentially.
     */
    public suspend fun dispatch(batch: Payload.Batch): List<ResponseMessage> =
        batch.entries.mapNotNull { entry ->
            if (entry == null) Payload.Invalid.INVALID_REQUEST.response else dispatch(entry)
        }

    /**
     * Register a pending request that expects a response.
     * Returns a Deferred that will complete when the response arrives.
     */
    public fun registerPendingRequest(id: RequestId): CompletableDeferred<JsonElement?> = pending.register(id)

    /**
     * Cancel all pending requests. Called during connection shutdown.
     */
    public fun cancelAllPendingRequests() {
        pending.cancelAll()
    }

    /**
     * Cancel a pending request.
     */
    public fun cancelPendingRequest(id: RequestId) {
        pending.remove(id)?.cancel()
    }
}

/** The waiters of outbound requests to one peer, by id. */
internal class PendingRequests {
    private data class State(
        val closed: Boolean = false,
        val requests: PersistentMap<RequestId, CompletableDeferred<JsonElement?>> = persistentHashMapOf(),
    )

    private val state = MutableStateFlow(State())
    val count: Int get() = state.value.requests.size

    fun register(id: RequestId): CompletableDeferred<JsonElement?> {
        val deferred = CompletableDeferred<JsonElement?>()
        while (true) {
            val current = state.value
            check(!current.closed) { "Connection is closed" }
            check(id !in current.requests) { "Request ID is already pending: $id" }
            if (state.compareAndSet(current, current.copy(requests = current.requests.put(id, deferred)))) return deferred
        }
    }

    fun remove(
        id: RequestId,
        expected: CompletableDeferred<JsonElement?>? = null,
    ): CompletableDeferred<JsonElement?>? {
        while (true) {
            val current = state.value
            val deferred = current.requests[id] ?: return null
            if (expected != null && deferred !== expected) return null
            if (state.compareAndSet(current, current.copy(requests = current.requests.remove(id)))) return deferred
        }
    }

    /** Removes and cancels the waiter [expected] for [id]; returns false when it is no longer pending. */
    fun cancel(
        id: RequestId,
        expected: CompletableDeferred<JsonElement?>,
    ): Boolean {
        val waiter = remove(id, expected) ?: return false
        waiter.cancel()
        return true
    }

    /** Completes the waiter that [response] answers; a response without a waiter is ignored. */
    fun complete(response: ResponseMessage) {
        val id = response.id ?: return
        val deferred = remove(id) ?: return
        val error = response.error
        if (error != null) {
            deferred.completeExceptionally(JsonRpcException(error.code, error.message, error.data))
        } else {
            deferred.complete(response.result)
        }
    }

    fun cancelAll() = drain(close = false)

    fun close() = drain(close = true)

    private fun drain(close: Boolean) {
        while (true) {
            val current = state.value
            if (state.compareAndSet(current, State(closed = current.closed || close))) {
                current.requests.values.forEach { it.cancel() }
                return
            }
        }
    }
}
