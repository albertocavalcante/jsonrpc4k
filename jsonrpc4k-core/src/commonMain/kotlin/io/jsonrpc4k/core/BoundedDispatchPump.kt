package io.jsonrpc4k.core

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.coroutines.yield

internal enum class OfferResult { ACCEPTED, TERMINAL_TRANSITION, REJECTED }

internal interface DispatchOutput {
    val isTerminating: Boolean get() = false

    suspend fun send(
        response: ResponseMessage,
        onDequeued: () -> Unit,
    )

    /**
     * Sends the responses of one batch as a single JSON array frame; [onDequeued] runs when the outgoing
     * collector dequeues it. The pump never passes an empty list.
     */
    suspend fun sendBatch(
        responses: List<ResponseMessage>,
        onDequeued: () -> Unit,
    )

    fun offer(
        response: ResponseMessage,
        onDequeued: (() -> Unit)? = null,
    ): OfferResult

    fun fail(cause: Throwable)
}

/**
 * Owns bounded asynchronous application dispatch; only the scheduler and the control lane start handlers.
 *
 * Each entry of a JSON-RPC 2.0 batch is admitted as its own message. Responses and `$/cancelRequest`
 * take effect at admission, as in single frames. Requests and notifications take their own queue
 * slots and run under the same rules as single messages; requests are tracked for `$/cancelRequest`
 * and duplicate IDs. Instead of being sent, the reply of a request entry fills that entry's slot,
 * and a batch sender sends the array once every slot is filled, so the batch is still answered with
 * one frame. The frame keeps one byte reservation until that sender finishes. An invalid frame is
 * answered at admission through the control reserve, as a busy reply is.
 *
 * [endInput] stops admission and waits for the admitted work, bounded by
 * [BoundedDispatchPolicy.drainTimeout]. After the timeout it cancels the scheduler, the control lane
 * and every request that is still unanswered; a reply that was already claimed, and every batch
 * sender, still finishes, bounded by the connection, so a cancelled batch entry still reaches its
 * array.
 */
internal class BoundedDispatchPump(
    private val policy: BoundedDispatchPolicy,
    private val decoder: JsonRpcMessageDecoder,
    private val peer: PeerDispatcher,
    scope: CoroutineScope,
    private val output: DispatchOutput,
    limits: InboundResourceLimits? = null,
) {
    private sealed interface Event {
        val reservation: Reservation?

        data class Request(
            val message: RequestMessage,
            val token: Any,
            val cancellation: CompletableDeferred<Unit>,
            val replyDone: CompletableDeferred<Unit>,
            val slot: CompletableDeferred<ResponseMessage?>?,
            override val reservation: Reservation?,
        ) : Event

        data class Notification(
            val message: NotificationMessage,
            val slot: CompletableDeferred<ResponseMessage?>?,
            override val reservation: Reservation?,
        ) : Event
    }

    private data class Running(
        val finalized: CompletableDeferred<Unit>,
    )

    private val admission = Mutex()
    private val connectionJob = checkNotNull(scope.coroutineContext[Job])
    private val work = SupervisorJob(connectionJob)
    private val workScope = CoroutineScope(scope.coroutineContext + work)
    private val controlNotifications = policy.controlNotifications.toSet()
    private val queued = Channel<Event>(policy.maxQueuedMessages, onUndeliveredElement = ::discard)
    private val lane = Channel<Event.Notification>(policy.maxQueuedMessages, onUndeliveredElement = ::discard)
    private val bytes = ApplicationBytes(limits?.maxApplicationBodyBytes)
    internal val retainedApplicationBytes: Int get() = bytes.retained
    private val queueSlots = Semaphore(policy.maxQueuedMessages)
    private val requests = AdmittedRequests()
    private val permits = Channel<Unit>(policy.maxConcurrentRequests)
    private val batchesInFlight = MutableStateFlow(0)
    private val closed = CompletableDeferred<Unit>()

    /** Completed when the admitted work has drained or [abandon] gave up on it. */
    private val settled = CompletableDeferred<Unit>()
    private val abandoned = MutableStateFlow(false)
    private var inputEnded = false
    private var drain: Job? = null

    /** Whether a failure of pump work is still this pump's to report. */
    private val ownsFailures: Boolean get() = connectionJob.isActive && !abandoned.value

    private val scheduler: Job
    private val controlLane: Job

    init {
        repeat(policy.maxConcurrentRequests) { check(permits.trySend(Unit).isSuccess) }
        scheduler = workScope.launchGuarded({ ownsFailures }, output::fail) { schedule() }
        controlLane =
            workScope.launchGuarded({ ownsFailures }, output::fail) {
                for (event in lane) runNotification(event, barrier = emptyList())
            }
    }

    suspend fun receive(data: ByteArray) {
        admission.withLock {
            check(!inputEnded) { "Inbound input has ended" }
            try {
                var offset = 0
                while (offset < data.size) {
                    val end = minOf(data.size, offset + minOf(data.size - offset, INPUT_SLICE_SIZE))
                    decoder.feedEach(data.copyOfRange(offset, end)) { payload, bodyBytes ->
                        currentCoroutineContext().ensureActive()
                        admit(payload, bodyBytes)
                    }
                    offset = end
                    if (offset < data.size) yield()
                }
            } catch (cancelled: CancellationException) {
                if (connectionJob.isActive) output.fail(cancelled)
                throw cancelled
            } catch (failure: Throwable) {
                output.fail(failure)
                throw failure
            }
        }
    }

    /**
     * Stops admission and waits until the admitted work is done: queued messages have run, and every
     * admitted request has been answered and its response dequeued. Returns at once when the pump is
     * closed or an earlier call already settled. When [BoundedDispatchPolicy.drainTimeout] elapses
     * first, abandons what remains. Must not be called from a handler of this pump, which would wait
     * for itself until the timeout.
     */
    suspend fun endInput() {
        admission.withLock {
            if (!inputEnded) {
                inputEnded = true
                queued.close()
                lane.close()
                drain =
                    workScope.launch {
                        scheduler.join()
                        controlLane.join()
                        batchesInFlight.first { it == 0 }
                        requests.awaitEmpty()
                        settled.complete(Unit)
                    }
            }
        }
        val done =
            withTimeoutOrNull(policy.drainTimeout) {
                select<Unit> {
                    settled.onAwait { }
                    closed.onAwait { }
                }
            }
        if (done == null && !settled.isCompleted) abandon()
    }

    fun close() {
        closed.complete(Unit)
        queued.cancel()
        lane.cancel()
        requests.close().forEach {
            it.cancellation.complete(Unit)
            it.replyDone.complete(Unit)
        }
    }

    fun outputInterrupted(cause: Throwable) = output.fail(cause)

    /**
     * Gives up on the drain: cancels the scheduler, the control lane and the queued messages, and then
     * every admitted request that is still unanswered, as a `$/cancelRequest` would. Request workers
     * that already claimed their reply and batch senders are left to finish, bounded by the
     * connection, so replies already produced and every batch array still reach the outgoing queue.
     */
    private fun abandon() {
        if (!abandoned.compareAndSet(false, true)) return
        drain?.cancel()
        scheduler.cancel()
        controlLane.cancel()
        queued.cancel()
        lane.cancel()
        requests.ids.forEach(::cancelRequest)
        settled.complete(Unit)
    }

    /** Releases a message that never ran; a notification batch entry has nothing to put in the array. */
    private fun discard(event: Event) {
        event.reservation?.release()
        if (event is Event.Notification) event.slot?.complete(null)
    }

    private fun admit(
        payload: Payload,
        bodyBytes: Int,
    ) {
        if (output.isTerminating) throw CancellationException("Connection is terminating")
        when (payload) {
            is Payload.Single -> admitMessage(payload.message, bodyBytes, slot = null)
            is Payload.Batch -> admitBatch(payload, bodyBytes)
            is Payload.Invalid -> {
                if (output.offer(payload.response) == OfferResult.REJECTED) {
                    throw JsonRpcProtocolException("Outgoing response queue is full")
                }
            }
        }
    }

    /** Admits one message; a batch entry has a [slot] for its reply and is covered by its frame's bytes. */
    private fun admitMessage(
        message: Message,
        bodyBytes: Int,
        slot: CompletableDeferred<ResponseMessage?>?,
    ) {
        when (message) {
            is ResponseMessage -> peer.pending.complete(message)
            is RequestMessage -> admitRequest(message, bodyBytes, slot)
            is NotificationMessage ->
                when (message.method) {
                    Dispatcher.CANCEL_REQUEST_METHOD -> cancelledRequestId(message.params)?.let(::cancelRequest)
                    in controlNotifications -> admitNotification(lane, message, bodyBytes, slot)
                    else -> admitNotification(queued, message, bodyBytes, slot)
                }
        }
    }

    private fun admitBatch(
        batch: Payload.Batch,
        bodyBytes: Int,
    ) {
        val (control, entries) = batch.entries.partition { it.isAdmissionControl() }
        for (message in control.filterNotNull()) admitMessage(message, bodyBytes = 0, slot = null)
        if (entries.isEmpty()) return
        val reservation = bytes.reserve(bodyBytes) ?: throw JsonRpcProtocolException("Incoming application byte budget is full")
        val slots = List(entries.size) { CompletableDeferred<ResponseMessage?>() }
        val tracked = mutableListOf<Pair<RequestId, Any>>()
        try {
            entries.forEachIndexed { index, entry ->
                when (entry) {
                    null -> slots[index].complete(Payload.Invalid.INVALID_REQUEST.response)
                    is RequestMessage -> admitRequest(entry, bodyBytes = 0, slots[index])?.let { tracked.add(entry.id to it) }
                    else -> admitMessage(entry, bodyBytes = 0, slots[index])
                }
            }
        } catch (failure: Throwable) {
            reservation.release()
            throw failure
        }
        // Constructed from ids and tokens only, so queued output does not retain the batch payload.
        val release = { tracked.forEach { (id, token) -> requests.remove(id, token) } }
        batchesInFlight.update { it + 1 }
        val sender =
            workScope.launchGuarded({ ownsFailures }, output::fail) {
                val responses = slots.mapNotNull { it.await() }
                if (responses.isEmpty()) release() else output.sendBatch(responses, release)
            }
        sender.invokeOnCompletion {
            reservation.release()
            batchesInFlight.update { it - 1 }
        }
    }

    /** Queues a notification on [channel]: the ordered queue, where it is a barrier, or the control lane. */
    private fun admitNotification(
        channel: SendChannel<Event.Notification>,
        message: NotificationMessage,
        bodyBytes: Int,
        slot: CompletableDeferred<ResponseMessage?>?,
    ) {
        val reservation =
            (if (slot == null) bytes.reserve(bodyBytes) else bytes.unlimited)
                ?: throw JsonRpcProtocolException("Incoming application byte budget is full")
        val acquired = queueSlots.tryAcquire()
        if (!acquired || !channel.trySend(Event.Notification(message, slot, reservation)).isSuccess) {
            if (acquired) queueSlots.release()
            reservation.release()
            throw JsonRpcProtocolException("Incoming ordered queue is full or closed")
        }
    }

    /** Admits a request and returns its tracking token, or null when it was dropped untracked. */
    private fun admitRequest(
        message: RequestMessage,
        bodyBytes: Int,
        slot: CompletableDeferred<ResponseMessage?>?,
    ): Any? {
        val id = message.id
        val token = Any()
        val cancellation = CompletableDeferred<Unit>()
        val replyDone = CompletableDeferred<Unit>()
        requests.add(id, AdmittedRequests.Entry(token, cancellation, replyDone, slot))
        val reservation = if (slot == null) bytes.reserve(bodyBytes) else bytes.unlimited
        val admitted = reservation != null && queueSlots.tryAcquire()
        if (!admitted || !queued.trySend(Event.Request(message, token, cancellation, replyDone, slot, reservation)).isSuccess) {
            if (admitted) queueSlots.release()
            reservation?.release()
            if (!connectionJob.isActive) {
                requests.remove(id, token)
                return null
            }
            if (requests.claimReply(id, token) &&
                reply(id, token, slot, ResponseMessage.error(id, ResponseError(SERVER_BUSY, "Server busy"))) == OfferResult.REJECTED
            ) {
                throw JsonRpcProtocolException("Outgoing response queue is full")
            }
        }
        return token
    }

    /** Delivers a reply that the pump writes itself: into the batch [slot], or as an immediate control frame. */
    private fun reply(
        id: RequestId,
        token: Any,
        slot: CompletableDeferred<ResponseMessage?>?,
        response: ResponseMessage,
    ): OfferResult {
        if (slot == null) return output.offer(response, requests.removal(id, token))
        slot.complete(response)
        return OfferResult.ACCEPTED
    }

    private fun cancelRequest(id: RequestId) {
        val entry = requests.claimCancel(id) ?: return
        val offered = reply(id, entry.token, entry.slot, ResponseMessage.error(id, ResponseError.requestCancelled()))
        if (offered == OfferResult.REJECTED && ownsFailures) {
            output.fail(JsonRpcProtocolException("Outgoing response queue is full"))
        }
        entry.replyDone.complete(Unit)
        entry.cancellation.complete(Unit)
        entry.job?.cancel()
    }

    private suspend fun schedule() {
        val running = mutableListOf<Running>()
        for (event in queued) {
            running.removeAll { it.finalized.isCompleted }
            when (event) {
                is Event.Notification -> {
                    runNotification(event, barrier = running)
                    running.clear()
                }
                is Event.Request -> {
                    startRequest(event)?.let(running::add)
                }
            }
        }
        // Input ended: the queue is drained, so let the admitted requests finish.
        running.map { it.finalized }.joinAll()
    }

    /** Runs a dequeued notification once the requests in [barrier] have finished. */
    private suspend fun runNotification(
        event: Event.Notification,
        barrier: List<Running>,
    ) {
        try {
            barrier.map { it.finalized }.joinAll()
            queueSlots.release()
            peer.dispatcher.handleNotification(event.message)
        } finally {
            event.reservation?.release()
            event.slot?.complete(null)
        }
    }

    private suspend fun startRequest(event: Event.Request): Running? {
        val acquired =
            try {
                select<Boolean> {
                    permits.onReceive { true }
                    event.cancellation.onAwait { false }
                }
            } catch (failure: Throwable) {
                event.reservation?.release()
                throw failure
            }
        queueSlots.release()
        if (!acquired) {
            event.reservation?.release()
            return null
        }
        val finalized = CompletableDeferred<Unit>()
        val worker = workScope.launch(start = CoroutineStart.LAZY) { runRequest(event) }
        worker.invokeOnCompletion { cause -> finishRequest(event, cause, finalized) }
        if (requests.attach(event.message.id, event.token, worker)) worker.start() else worker.cancel()
        return Running(finalized)
    }

    private suspend fun runRequest(event: Event.Request) {
        try {
            // This pump cancels its own workers, so the dispatcher does not track the handler.
            val response = peer.dispatcher.handleRequest(event.message, inbound = null)
            val id = event.message.id
            val token = event.token
            if (!requests.claimReply(id, token)) return
            try {
                if (event.slot != null) event.slot.complete(response) else output.send(response, requests.removal(id, token))
            } finally {
                event.replyDone.complete(Unit)
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            if (connectionJob.isActive) output.fail(failure)
        } catch (fatal: Throwable) {
            if (connectionJob.isActive) output.fail(fatal)
            throw fatal
        }
    }

    private fun finishRequest(
        event: Event.Request,
        cause: Throwable?,
        finalized: CompletableDeferred<Unit>,
    ) {
        val id = event.message.id
        val token = event.token
        requests.detach(id, token)
        event.reservation?.release()
        if (cause is CancellationException && connectionJob.isActive && requests.claimReply(id, token)) {
            val offered = reply(id, token, event.slot, ResponseMessage.error(id, ResponseError.requestCancelled()))
            if (offered == OfferResult.REJECTED && ownsFailures) output.fail(JsonRpcProtocolException("Outgoing response queue is full"))
            event.replyDone.complete(Unit)
        } else if (cause != null && cause !is CancellationException && connectionJob.isActive) {
            output.fail(cause)
            event.replyDone.complete(Unit)
        } else if (!connectionJob.isActive) {
            event.replyDone.complete(Unit)
        }
        event.replyDone.invokeOnCompletion {
            permits.trySend(Unit)
            finalized.complete(Unit)
        }
    }

    private companion object {
        const val INPUT_SLICE_SIZE = 16 * 1024
        const val SERVER_BUSY = -32000
    }
}

/** Releases its bytes at most once. */
private class Reservation(
    private val releaseBytes: () -> Unit,
) {
    private val released = MutableStateFlow(false)

    fun release() {
        if (released.compareAndSet(false, true)) releaseBytes()
    }
}

/** The inbound application byte budget; without a [maximum] every reservation succeeds. */
private class ApplicationBytes(
    private val maximum: Int?,
) {
    private val state = MutableStateFlow(0)
    val retained: Int get() = state.value

    /** A reservation that holds no bytes, for work that a frame's reservation already covers. */
    val unlimited = Reservation {}

    fun reserve(bytes: Int): Reservation? {
        val maximum = maximum ?: return unlimited
        while (true) {
            val current = state.value
            if (bytes > maximum - current) return null
            if (state.compareAndSet(current, current + bytes)) {
                return Reservation {
                    while (true) {
                        val before = state.value
                        if (state.compareAndSet(before, before - bytes)) break
                    }
                }
            }
        }
    }
}

/**
 * The incoming requests that a bounded pump admitted, by id. An entry stays until the response that
 * answers it is dequeued by the outgoing collector, so its id stays active until then.
 */
private class AdmittedRequests {
    data class Entry(
        val token: Any,
        val cancellation: CompletableDeferred<Unit>,
        val replyDone: CompletableDeferred<Unit>,
        val slot: CompletableDeferred<ResponseMessage?>?,
        val cancelled: Boolean = false,
        val replied: Boolean = false,
        val job: Job? = null,
    )

    private data class State(
        val closed: Boolean = false,
        val requests: PersistentMap<RequestId, Entry> = persistentHashMapOf(),
    )

    private val state = MutableStateFlow(State())

    val ids: Set<RequestId> get() = state.value.requests.keys

    suspend fun awaitEmpty() {
        state.first { it.requests.isEmpty() }
    }

    /** Tracks [entry] as the request [id]; a duplicate active id fails the connection. */
    fun add(
        id: RequestId,
        entry: Entry,
    ) {
        while (true) {
            val current = state.value
            check(!current.closed) { "Connection is closed" }
            if (id in current.requests) throw JsonRpcProtocolException("Duplicate active incoming request ID")
            if (state.compareAndSet(current, current.copy(requests = current.requests.put(id, entry)))) return
        }
    }

    /** Claims the only reply of the request; false when it was already claimed or is no longer tracked. */
    fun claimReply(
        id: RequestId,
        token: Any,
    ): Boolean = update(id, token) { if (it.replied) null else it.copy(replied = true) } != null

    /** Claims the reply of the request [id] for a cancellation; returns its entry, or null when already replied. */
    fun claimCancel(id: RequestId): Entry? =
        update(id, token = null) { if (it.replied) null else it.copy(cancelled = true, replied = true) }

    fun attach(
        id: RequestId,
        token: Any,
        job: Job,
    ): Boolean = update(id, token) { if (it.cancelled) null else it.copy(job = job) } != null

    fun detach(
        id: RequestId,
        token: Any,
    ) {
        update(id, token) { if (it.job == null) null else it.copy(job = null) }
    }

    fun remove(
        id: RequestId,
        token: Any,
    ) {
        while (true) {
            val current = state.value
            if (current.requests[id]?.token !== token) return
            if (state.compareAndSet(current, current.copy(requests = current.requests.remove(id)))) return
        }
    }

    /** Constructed without a request/event/entry so queued output does not retain its payload. */
    fun removal(
        id: RequestId,
        token: Any,
    ): () -> Unit = { remove(id, token) }

    /** Stops tracking and returns the entries that were tracked. */
    fun close(): Collection<Entry> {
        while (true) {
            val current = state.value
            if (state.compareAndSet(current, State(closed = true))) return current.requests.values
        }
    }

    /**
     * Replaces the entry for [id] with [change] when [token] (if given) owns it and [change] accepts it.
     * Returns the entry before the change, or null when nothing changed.
     */
    private inline fun update(
        id: RequestId,
        token: Any?,
        change: (Entry) -> Entry?,
    ): Entry? {
        while (true) {
            val current = state.value
            val entry = current.requests[id] ?: return null
            if (token != null && entry.token !== token) return null
            val next = change(entry) ?: return null
            if (state.compareAndSet(current, current.copy(requests = current.requests.put(id, next)))) return entry
        }
    }
}

/** Launches pump work that reports an unexpected end through [fail] while [owned] holds. */
private fun CoroutineScope.launchGuarded(
    owned: () -> Boolean,
    fail: (Throwable) -> Unit,
    block: suspend () -> Unit,
): Job =
    launch {
        try {
            block()
        } catch (cancelled: CancellationException) {
            if (owned()) fail(cancelled)
            throw cancelled
        } catch (failure: Exception) {
            if (owned()) fail(failure)
        } catch (fatal: Throwable) {
            if (owned()) fail(fatal)
            throw fatal
        }
    }

/** Batch entries that a bounded pump handles at admission instead of queueing them. */
private fun Message?.isAdmissionControl(): Boolean =
    this is ResponseMessage || (this is NotificationMessage && method == Dispatcher.CANCEL_REQUEST_METHOD)
