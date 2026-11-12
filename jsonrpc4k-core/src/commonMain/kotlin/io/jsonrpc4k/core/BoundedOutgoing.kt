package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex

/** All accepted items own a slot until delivered or discarded, including in-flight emits. */
internal class BoundedOutgoing(
    private val limits: OutboundResourceLimits,
    private val haltIngress: () -> Unit = {},
    private val fail: (Throwable) -> Unit,
) {
    private data class State(
        val ordinaryBytes: Int = 0,
        val ordinaryCount: Int = 0,
        val controlBytes: Int = 0,
        val controlCount: Int = 0,
        val producers: Int = 0,
        val controlEncoding: Boolean = false,
        val deliveryLost: Boolean = false,
        val terminalClaim: Throwable? = null,
        val failure: Throwable? = null,
    )

    internal sealed interface Item {
        val lease: Lease
        data class Frame(val bytes: ByteArray, val onDequeued: (() -> Unit)?, override val lease: Lease) : Item
        data class Barrier(val completion: CompletableDeferred<Unit>, override val lease: Lease) : Item
    }

    internal class Lease(private val release: () -> Unit) {
        private val released = MutableStateFlow(false)
        fun release() {
            if (released.compareAndSet(false, true)) release.invoke()
        }
    }

    private val state = MutableStateFlow(State())
    private val ended = CompletableDeferred<Unit>()
    private val admission = Mutex()
    private val encoder = BoundedMessageEncoder(limits.maxFrameWireBytes, limits.maxNodes, limits.maxJsonNesting)
    val items = Channel<Item>(limits.maxOutstandingFrames + limits.controlReserveFrames, onUndeliveredElement = { item ->
        // A promptly-cancelled receive may lose its assigned item before the collector catches
        // cancellation. Block new reservations until that catch installs the terminal cause.
        var won = false
        mutate { s ->
            if (s.failure == null && s.terminalClaim == null && !s.deliveryLost) {
                won = true
                s.copy(deliveryLost = true)
            } else {
                won = false
                s
            }
        }
        try {
            if (won) haltIngress()
        } finally {
            item.lease.release()
        }
    })

    fun close(cause: Throwable) {
        while (true) {
            val old = state.value
            if (old.failure != null) return
            if (state.compareAndSet(old, old.copy(failure = cause))) {
                ended.completeExceptionally(cause)
                items.cancel()
                return
            }
        }
    }

    internal val outstandingCount: Int get() = state.value.ordinaryCount + state.value.controlCount
    internal val outstandingBytes: Int get() = state.value.ordinaryBytes + state.value.controlBytes

    fun claimFailure(cause: Throwable): Boolean {
        while (true) {
            val current = state.value
            if (current.deliveryLost || current.failure != null) return false
            val claimed = current.terminalClaim
            if (claimed != null) return claimed === cause
            if (state.compareAndSet(current, current.copy(terminalClaim = cause))) return true
        }
    }

    fun resolveCollectorLoss(cause: Throwable) {
        mutate { s ->
            if (s.deliveryLost && s.failure == null) s.copy(deliveryLost = false, terminalClaim = cause) else s
        }
    }

    private fun claimControlFailure(cause: Throwable): Boolean {
        while (true) {
            val old = state.value
            if (old.deliveryLost) return false
            if (old.failure != null || old.terminalClaim != null) return false
            if (state.compareAndSet(old, old.copy(terminalClaim = cause))) {
                haltIngress()
                return true
            }
        }
    }

    private fun mutate(change: (State) -> State): State {
        while (true) {
            val old = state.value
            val next = change(old)
            if (state.compareAndSet(old, next)) return next
        }
    }

    private fun beginProducer() {
        mutate { s ->
            s.failure?.let { throw it }
            check(s.producers < limits.maxPendingProducers) { "Outbound producer limit exceeded" }
            s.copy(producers = s.producers + 1)
        }
    }

    private suspend fun <T> admitted(block: suspend () -> T): T {
        beginProducer()
        try {
            select<Unit> {
                admission.onLock { }
                ended.onAwait { }
            }
            try {
                state.value.failure?.let { throw it }
                return block()
            } finally {
                admission.unlock()
            }
        } finally {
            mutate { it.copy(producers = it.producers - 1) }
        }
    }

    private fun reserve(bytes: Int, control: Boolean): Lease? {
        while (true) {
            val old = state.value
            (old.failure ?: old.terminalClaim)?.let { throw it }
            if (old.deliveryLost) return null
            val next = if (control) {
                if (old.controlCount == limits.controlReserveFrames || bytes > limits.controlReserveBytes - old.controlBytes) return null
                old.copy(controlCount = old.controlCount + 1, controlBytes = old.controlBytes + bytes)
            } else {
                if (old.ordinaryCount == limits.maxOutstandingFrames || bytes > limits.maxRetainedWireBytes - old.ordinaryBytes) return null
                old.copy(ordinaryCount = old.ordinaryCount + 1, ordinaryBytes = old.ordinaryBytes + bytes)
            }
            if (state.compareAndSet(old, next)) {
                return Lease {
                    mutate { s ->
                        if (control) s.copy(controlCount = s.controlCount - 1, controlBytes = s.controlBytes - bytes)
                        else s.copy(ordinaryCount = s.ordinaryCount - 1, ordinaryBytes = s.ordinaryBytes - bytes)
                    }
                }
            }
        }
    }

    private suspend fun reserveOrdinary(bytes: Int): Lease {
        while (true) {
            reserve(bytes, false)?.let { return it }
            state.first { s -> s.failure != null || s.terminalClaim != null || (!s.deliveryLost && s.ordinaryCount < limits.maxOutstandingFrames && bytes <= limits.maxRetainedWireBytes - s.ordinaryBytes) }
        }
    }

    private fun transfer(item: Item) {
        val result = items.trySend(item)
        if (result.isFailure) {
            item.lease.release()
            state.value.failure?.let { throw it }
            error("Outbound queue closed")
        }
    }

    suspend fun send(message: Message, onDequeued: (() -> Unit)? = null) = admitted {
        val bytes = encoder.encode(message)
        transfer(Item.Frame(bytes, onDequeued, reserveOrdinary(bytes.size)))
    }

    suspend fun barrier(completion: CompletableDeferred<Unit>, drain: OutgoingDrain) = admitted {
        drain.register(completion)
        transfer(Item.Barrier(completion, reserveOrdinary(0)))
    }

    private fun beginControl(): Boolean {
        while (true) {
            val old = state.value
            (old.failure ?: old.terminalClaim)?.let { throw it }
            // The collector is unwinding and owns the terminal cause.
            if (old.deliveryLost) return false
            check(!old.controlEncoding) { "Outbound control encoder busy" }
            if (state.compareAndSet(old, old.copy(controlEncoding = true))) return true
        }
    }

    fun offer(message: Message, onDequeued: (() -> Unit)?): OfferResult {
        try {
            if (!beginControl()) return OfferResult.TERMINAL_TRANSITION
            try {
                val bytes = encoder.encode(message)
                val lease = reserve(bytes.size, true)
                if (lease == null) {
                    if (state.value.deliveryLost) return OfferResult.TERMINAL_TRANSITION
                    error("Outbound control capacity exceeded")
                }
                transfer(Item.Frame(bytes, onDequeued, lease))
            } finally {
                mutate { it.copy(controlEncoding = false) }
            }
            return OfferResult.ACCEPTED
        } catch (cause: Exception) {
            if (!claimControlFailure(cause)) return OfferResult.TERMINAL_TRANSITION
            fail(cause)
            return OfferResult.REJECTED
        } catch (fatal: Throwable) {
            if (claimControlFailure(fatal)) fail(fatal)
            throw fatal
        }
    }
}
