package io.jsonrpc4k.core

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.job
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.coroutines.cancellation.CancellationException

/**
 * The inbound requests whose handlers are running, by id, so that `$/cancelRequest` can stop them.
 *
 * This registry is directional: it holds only requests received from the peer, never the outbound
 * waiters of [PendingRequests], so a remote cancellation cannot complete a local request that has
 * the same id. A duplicate of a running id runs untracked. A cancelled id is released at once, so
 * the peer may reuse it while the old handler is still finishing.
 */
internal class InboundRequests {
    private class Entry(
        val job: Job,
        val cancelled: MutableStateFlow<Boolean>,
    )

    private val running = MutableStateFlow<PersistentMap<RequestId, Entry>>(persistentHashMapOf())

    /** The number of tracked handlers. */
    val count: Int get() = running.value.size

    /**
     * Runs [block] as the handler of the inbound request [id], in a child job that a remote
     * cancellation of [id] cancels. A cancelled [block] then ends with a [JsonRpcException] that
     * carries [ResponseError.REQUEST_CANCELLED], unless it throws an exception of its own or the
     * calling coroutine is cancelled as well.
     */
    suspend fun <T> runCancellable(
        id: RequestId,
        block: suspend () -> T,
    ): T {
        val cancelled = MutableStateFlow(false)
        try {
            return coroutineScope {
                val entry = Entry(coroutineContext.job, cancelled)
                val tracked = add(id, entry)
                try {
                    block()
                } finally {
                    if (tracked) remove(id, entry)
                }
            }
        } catch (cause: CancellationException) {
            if (!cancelled.value) throw cause
            // The caller's own cancellation takes precedence over the remote one.
            currentCoroutineContext().ensureActive()
            throw JsonRpcException(ResponseError.REQUEST_CANCELLED, "Request cancelled")
        }
    }

    /** Cancels the handler running for [id] and releases the id; returns false when none is tracked. */
    fun cancel(id: RequestId): Boolean {
        while (true) {
            val current = running.value
            val entry = current[id] ?: return false
            if (running.compareAndSet(current, current.remove(id))) {
                entry.cancelled.value = true
                entry.job.cancel()
                return true
            }
        }
    }

    private fun add(
        id: RequestId,
        entry: Entry,
    ): Boolean {
        while (true) {
            val current = running.value
            if (id in current) return false
            if (running.compareAndSet(current, current.put(id, entry))) return true
        }
    }

    private fun remove(
        id: RequestId,
        entry: Entry,
    ) {
        while (true) {
            val current = running.value
            if (current[id] !== entry) return
            if (running.compareAndSet(current, current.remove(id))) return
        }
    }
}

/** The request id that `$/cancelRequest` params name, or null when they name no number or string id. */
internal fun cancelledRequestId(params: JsonElement?): RequestId? {
    val primitive = (params as? JsonObject)?.get("id") as? JsonPrimitive ?: return null
    return if (primitive.isString) RequestId.of(primitive.content) else primitive.content.toLongOrNull()?.let(RequestId::of)
}
