package io.jsonrpc4k.core

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

internal interface DispatchOutput {
    suspend fun send(response: ResponseMessage, onDequeued: () -> Unit)

    fun offer(response: ResponseMessage, onDequeued: (() -> Unit)? = null): Boolean

    fun fail(cause: Throwable)
}

/** Owns bounded asynchronous application dispatch; only the scheduler starts handlers. */
internal class BoundedDispatchPump(
    private val policy: BoundedDispatchPolicy,
    private val decoder: JsonRpcMessageDecoder,
    private val dispatcher: Dispatcher,
    private val scope: CoroutineScope,
    private val output: DispatchOutput,
) {
    private data class Incoming(
        val token: Any,
        val cancellation: CompletableDeferred<Unit>,
        val replyDone: CompletableDeferred<Unit>,
        val cancelled: Boolean = false,
        val replied: Boolean = false,
        val job: Job? = null,
    )

    private data class IncomingState(
        val closed: Boolean = false,
        val requests: PersistentMap<RequestId, Incoming> = persistentHashMapOf(),
    )

    private sealed interface Event {
        data class Request(
            val message: RequestMessage,
            val token: Any,
            val cancellation: CompletableDeferred<Unit>,
            val replyDone: CompletableDeferred<Unit>,
        ) : Event

        data class Notification(val message: NotificationMessage) : Event
    }

    private data class Running(val finalized: CompletableDeferred<Unit>)

    private val admission = Mutex()
    private val connectionJob = checkNotNull(scope.coroutineContext[Job])
    private val queued = Channel<Event>(policy.maxQueuedMessages)
    private val queueSlots = Semaphore(policy.maxQueuedMessages)
    private val incoming = MutableStateFlow(IncomingState())
    private val permits = Channel<Unit>(policy.maxConcurrentRequests)

    init {
        repeat(policy.maxConcurrentRequests) { check(permits.trySend(Unit).isSuccess) }
        scope.launch {
            try {
                schedule()
            } catch (cancelled: CancellationException) {
                if (connectionJob.isActive) output.fail(cancelled)
                throw cancelled
            } catch (failure: Exception) {
                if (connectionJob.isActive) output.fail(failure)
            } catch (fatal: Throwable) {
                if (connectionJob.isActive) output.fail(fatal)
                throw fatal
            }
        }
    }

    suspend fun receive(data: ByteArray) {
        try {
            admission.withLock {
                var offset = 0
                while (offset < data.size) {
                    val end = minOf(data.size, offset + minOf(data.size - offset, INPUT_SLICE_SIZE))
                    for (message in decoder.feed(data.copyOfRange(offset, end))) admit(message)
                    offset = end
                }
            }
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (failure: Exception) {
            output.fail(failure)
            throw failure
        } catch (fatal: Throwable) {
            output.fail(fatal)
            throw fatal
        }
    }

    fun close() {
        queued.cancel()
        while (true) {
            val current = incoming.value
            if (incoming.compareAndSet(current, IncomingState(closed = true))) {
                current.requests.values.forEach {
                    it.cancellation.complete(Unit)
                    it.replyDone.complete(Unit)
                }
                return
            }
        }
    }

    fun outputInterrupted(cause: Throwable) = output.fail(cause)

    private suspend fun admit(message: Message) {
        when (message) {
            is ResponseMessage -> dispatcher.dispatch(message)
            is RequestMessage -> admitRequest(message)
            is NotificationMessage -> {
                if (message.method == Dispatcher.CANCEL_REQUEST_METHOD) {
                    cancel(message)
                } else {
                    if (!queueSlots.tryAcquire()) throw JsonRpcProtocolException("Incoming notification queue is full")
                    if (!queued.trySend(Event.Notification(message)).isSuccess) {
                        queueSlots.release()
                        throw JsonRpcProtocolException("Incoming notification queue is closed")
                    }
                }
            }
        }
    }

    private fun admitRequest(message: RequestMessage) {
        val token = Any()
        val cancellation = CompletableDeferred<Unit>()
        val replyDone = CompletableDeferred<Unit>()
        while (true) {
            val current = incoming.value
            check(!current.closed) { "Connection is closed" }
            if (message.id in current.requests) throw JsonRpcProtocolException("Duplicate active incoming request ID")
            val requests = current.requests.put(message.id, Incoming(token, cancellation, replyDone))
            if (incoming.compareAndSet(current, current.copy(requests = requests))) break
        }
        val admitted = queueSlots.tryAcquire()
        if (!admitted || !queued.trySend(Event.Request(message, token, cancellation, replyDone)).isSuccess) {
            if (admitted) queueSlots.release()
            if (!connectionJob.isActive) {
                remove(message.id, token)
                return
            }
            if (!claimReply(message.id, token)) return
            val offered = output.offer(ResponseMessage.error(message.id, ResponseError(SERVER_BUSY, "Server busy"))) {
                remove(message.id, token)
            }
            if (!offered) {
                throw JsonRpcProtocolException("Outgoing response queue is full")
            }
        }
    }

    private fun cancel(message: NotificationMessage) {
        val primitive = (message.params as? JsonObject)?.get("id") as? JsonPrimitive ?: return
        val id = if (primitive.isString) RequestId.of(primitive.content) else primitive.content.toLongOrNull()?.let(RequestId::of) ?: return
        while (true) {
            val current = incoming.value
            val entry = current.requests[id] ?: return
            if (entry.replied) return
            val requests = current.requests.put(id, entry.copy(cancelled = true, replied = true))
            if (incoming.compareAndSet(current, current.copy(requests = requests))) {
                val offered = output.offer(ResponseMessage.error(id, ResponseError.requestCancelled())) { remove(id, entry.token) }
                if (!offered && connectionJob.isActive) output.fail(JsonRpcProtocolException("Outgoing response queue is full"))
                entry.replyDone.complete(Unit)
                entry.cancellation.complete(Unit)
                entry.job?.cancel()
                return
            }
        }
    }

    private suspend fun schedule() {
        val running = mutableListOf<Running>()
        for (event in queued) {
            running.removeAll { it.finalized.isCompleted }
            when (event) {
                is Event.Notification -> {
                    running.map { it.finalized }.joinAll()
                    running.clear()
                    queueSlots.release()
                    dispatcher.dispatch(event.message)
                }
                is Event.Request -> {
                    startRequest(event)?.let(running::add)
                }
            }
        }
    }

    private suspend fun startRequest(event: Event.Request): Running? {
        val acquired = select<Boolean> {
            permits.onReceive { true }
            event.cancellation.onAwait { false }
        }
        queueSlots.release()
        if (!acquired) return null
        val finalized = CompletableDeferred<Unit>()
        val worker = scope.launch(start = CoroutineStart.LAZY) { runRequest(event) }
        worker.invokeOnCompletion { cause -> finishRequest(event, cause, finalized) }
        if (attach(event.message.id, event.token, worker)) worker.start() else worker.cancel()
        return Running(finalized)
    }

    private suspend fun runRequest(event: Event.Request) {
        try {
            val response = checkNotNull(dispatcher.dispatch(event.message))
            if (claimReply(event.message.id, event.token)) {
                try {
                    output.send(response) { remove(event.message.id, event.token) }
                } finally {
                    event.replyDone.complete(Unit)
                }
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

    private fun finishRequest(event: Event.Request, cause: Throwable?, finalized: CompletableDeferred<Unit>) {
        if (cause is CancellationException && connectionJob.isActive && claimReply(event.message.id, event.token)) {
            val offered = output.offer(ResponseMessage.error(event.message.id, ResponseError.requestCancelled())) {
                remove(event.message.id, event.token)
            }
            if (!offered) output.fail(JsonRpcProtocolException("Outgoing response queue is full"))
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

    private fun claimReply(id: RequestId, token: Any): Boolean {
        while (true) {
            val current = incoming.value
            val entry = current.requests[id] ?: return false
            if (entry.token !== token || entry.replied) return false
            val requests = current.requests.put(id, entry.copy(replied = true))
            if (incoming.compareAndSet(current, current.copy(requests = requests))) return true
        }
    }

    private fun attach(id: RequestId, token: Any, job: Job): Boolean {
        while (true) {
            val current = incoming.value
            val entry = current.requests[id] ?: return false
            if (entry.token !== token || entry.cancelled) return false
            val requests = current.requests.put(id, entry.copy(job = job))
            if (incoming.compareAndSet(current, current.copy(requests = requests))) return true
        }
    }

    private fun remove(id: RequestId, token: Any) {
        while (true) {
            val current = incoming.value
            if (current.requests[id]?.token !== token) return
            if (incoming.compareAndSet(current, current.copy(requests = current.requests.remove(id)))) return
        }
    }

    private companion object {
        const val INPUT_SLICE_SIZE = 16 * 1024
        const val SERVER_BUSY = -32000
    }
}
