package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first

/** All accepted items own a slot until delivered or discarded, including in-flight emits. */
internal class BoundedOutgoing(
    private val limits: OutboundResourceLimits,
    private val haltIngress: () -> Unit = {},
    private val parentActive: () -> Boolean = { true },
    private val parentInactive: () -> Unit = {},
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

        data class Frame(
            val bytes: ByteArray,
            val onDequeued: (() -> Unit)?,
            override val lease: Lease,
        ) : Item

        data class Barrier(
            val completion: CompletableDeferred<Unit>,
            override val lease: Lease,
        ) : Item
    }

    internal class Lease(
        private val release: () -> Unit,
    ) {
        private val released = MutableStateFlow(false)

        fun release() {
            if (released.compareAndSet(false, true)) release.invoke()
        }
    }

    private val state = MutableStateFlow(State())
    private val ended = CompletableDeferred<Unit>()
    private val admission = OutboundAdmission(ended, fail)
    private val encoder = BoundedMessageEncoder(limits.maxFrameWireBytes, limits.maxNodes, limits.maxJsonNesting)
    val items =
        Channel<Item>(limits.maxOutstandingFrames + limits.controlReserveFrames, onUndeliveredElement = { item ->
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

    fun closeNormally(): Throwable? {
        while (true) {
            val old = state.value
            if (old.failure != null) return old.terminalClaim
            val cause = old.terminalClaim
            val closed = cause ?: IllegalStateException("Connection closed")
            if (state.compareAndSet(old, old.copy(failure = closed))) {
                ended.completeExceptionally(closed)
                items.cancel()
                return cause
            }
        }
    }

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

    val isTerminating: Boolean get() = state.value.let { it.deliveryLost || it.terminalClaim != null || it.failure != null }
    internal val outstandingCount: Int get() = state.value.ordinaryCount + state.value.controlCount
    internal val outstandingBytes: Int get() = state.value.ordinaryBytes + state.value.controlBytes

    fun claimFailure(
        cause: Throwable,
        control: Boolean = false,
    ): Boolean {
        while (true) {
            if (control && !parentActive()) {
                parentInactive()
                return false
            }
            val current = state.value
            if (current.deliveryLost || current.failure != null) return false
            val claimed = current.terminalClaim
            if (claimed != null) return !control && claimed === cause
            if (state.compareAndSet(current, current.copy(terminalClaim = cause))) {
                if (control) haltIngress()
                return true
            }
        }
    }

    fun resolveCollectorLoss(cause: Throwable) {
        mutate { s ->
            if (s.deliveryLost && s.failure == null) s.copy(deliveryLost = false, terminalClaim = cause) else s
        }
    }

    private fun mutate(change: (State) -> State): State {
        while (true) {
            val old = state.value
            val next = change(old)
            if (state.compareAndSet(old, next)) return next
        }
    }

    suspend fun <T> withProducer(block: suspend () -> T): T {
        mutate { s ->
            (s.failure ?: s.terminalClaim)?.let { throw it }
            check(s.producers < limits.maxPendingProducers) { "Outbound producer limit exceeded" }
            s.copy(producers = s.producers + 1)
        }
        try {
            val open = state.first { !it.deliveryLost || it.failure != null }
            (open.failure ?: open.terminalClaim)?.let { throw it }
            return block()
        } finally {
            mutate { it.copy(producers = it.producers - 1) }
        }
    }

    private suspend fun <T> admitted(block: suspend () -> T): T {
        admission.acquire()
        try {
            val current = state.value
            (current.failure ?: current.terminalClaim)?.let { throw it }
            return block()
        } finally {
            admission.release()
        }
    }

    private fun reserve(
        bytes: Int,
        control: Boolean,
    ): Lease? {
        while (true) {
            val old = state.value
            (old.failure ?: old.terminalClaim)?.let { throw it }
            if (old.deliveryLost) return null
            val next =
                if (control) {
                    val noSlot = old.controlCount == limits.controlReserveFrames
                    val overBudget = bytes > limits.controlReserveBytes - old.controlBytes
                    if (noSlot || overBudget) return null
                    old.copy(controlCount = old.controlCount + 1, controlBytes = old.controlBytes + bytes)
                } else {
                    val noSlot = old.ordinaryCount == limits.maxOutstandingFrames
                    val overBudget = bytes > limits.maxRetainedWireBytes - old.ordinaryBytes
                    if (noSlot || overBudget) return null
                    old.copy(ordinaryCount = old.ordinaryCount + 1, ordinaryBytes = old.ordinaryBytes + bytes)
                }
            if (state.compareAndSet(old, next)) {
                return Lease {
                    mutate { s ->
                        if (control) {
                            s.copy(controlCount = s.controlCount - 1, controlBytes = s.controlBytes - bytes)
                        } else {
                            s.copy(ordinaryCount = s.ordinaryCount - 1, ordinaryBytes = s.ordinaryBytes - bytes)
                        }
                    }
                }
            }
        }
    }

    private suspend fun reserveOrdinary(bytes: Int): Lease {
        while (true) {
            reserve(bytes, false)?.let { return it }
            state.first { s ->
                val hasSlot = s.ordinaryCount < limits.maxOutstandingFrames
                val hasBytes = bytes <= limits.maxRetainedWireBytes - s.ordinaryBytes
                s.failure != null || s.terminalClaim != null || (!s.deliveryLost && hasSlot && hasBytes)
            }
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

    suspend fun sendRegistered(
        message: Message,
        onDequeued: (() -> Unit)? = null,
    ) = admitted {
        val bytes = encoder.encode(message)
        transfer(Item.Frame(bytes, onDequeued, reserveOrdinary(bytes.size)))
    }

    /**
     * Queues [message] as an ordinary frame only when admission and capacity are free now, without
     * taking a producer slot, so it never waits behind or crowds out other producers. Returns false,
     * dropping the message, when it would have to wait or cannot be queued. It never throws, because
     * it runs in a caller's finally block, where a throw would hide that caller's own outcome.
     */
    fun trySend(
        message: Message,
        onDequeued: (() -> Unit)?,
    ): Boolean {
        if (!admission.tryAcquire()) return false
        return try {
            val bytes = encoder.encode(message)
            val lease = reserve(bytes.size, control = false)
            if (lease != null) transfer(Item.Frame(bytes, onDequeued, lease))
            lease != null
        } catch (_: Throwable) {
            false
        } finally {
            admission.release()
        }
    }

    /** Sends [messages] as one JSON-RPC 2.0 batch frame; the frame limits apply to the whole array. */
    suspend fun sendBatch(messages: List<Message>) =
        withProducer {
            admitted {
                val bytes = encoder.encode(messages)
                transfer(Item.Frame(bytes, null, reserveOrdinary(bytes.size)))
            }
        }

    suspend fun barrier(
        completion: CompletableDeferred<Unit>,
        drain: OutgoingDrain,
    ) = withProducer {
        admitted {
            drain.register(completion)
            transfer(Item.Barrier(completion, reserveOrdinary(0)))
        }
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

    private fun encodeControl(
        message: Message,
        onDequeued: (() -> Unit)?,
    ): OfferResult {
        val bytes = encoder.encode(message)
        val lease = reserve(bytes.size, true)
        if (lease != null) {
            transfer(Item.Frame(bytes, onDequeued, lease))
            return OfferResult.ACCEPTED
        }
        if (state.value.deliveryLost) return OfferResult.TERMINAL_TRANSITION
        error("Outbound control capacity exceeded")
    }

    fun offer(
        message: Message,
        onDequeued: (() -> Unit)?,
    ): OfferResult =
        try {
            if (!beginControl()) {
                OfferResult.TERMINAL_TRANSITION
            } else {
                try {
                    encodeControl(message, onDequeued)
                } finally {
                    mutate { it.copy(controlEncoding = false) }
                }
            }
        } catch (cause: Exception) {
            if (claimFailure(cause, control = true)) {
                fail(cause)
                OfferResult.REJECTED
            } else {
                OfferResult.TERMINAL_TRANSITION
            }
        } catch (fatal: Throwable) {
            if (claimFailure(fatal, control = true)) fail(fatal)
            throw fatal
        }
}

/**
 * Sends [message] as an ordinary frame: takes a producer slot, failing at once when none is free,
 * then waits for admission and capacity.
 */
internal suspend fun BoundedOutgoing.send(
    message: Message,
    onDequeued: (() -> Unit)? = null,
) = withProducer {
    sendRegistered(message, onDequeued)
}
