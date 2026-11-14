package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.serializer
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A bidirectional JSON-RPC connection.
 *
 * This class manages the full lifecycle of a JSON-RPC connection, including:
 * - Sending requests and awaiting responses
 * - Sending notifications (fire-and-forget)
 * - Receiving and dispatching incoming messages
 * - Request ID generation
 *
 * Each inbound frame holds one message or a JSON-RPC 2.0 batch (a JSON array). A batch is answered
 * with one array frame holding a response for each request and an Invalid Request error for each
 * invalid entry; a batch of only notifications or responses is answered with nothing. A frame that
 * is not valid JSON is answered with a Parse error, and an empty array or any other value that is
 * not a message object with an Invalid Request, each as a single response with a null id. The
 * connection stays open after both. In bounded mode each batch entry is dispatched as its own
 * message under the same limits (see [BoundedDispatchPolicy]): its requests run concurrently and can
 * be cancelled with `$/cancelRequest`, and the array is sent once every entry has finished.
 *
 * An inbound `$/cancelRequest` (LSP base protocol) cancels the handler of the request that it names.
 * The default mode runs each handler to completion inside [receive] before it reads the next message,
 * so a cancellation reaches only handlers that run concurrently, as in bounded mode. There the request
 * is answered with a Request Cancelled error (-32800) as soon as the cancellation is admitted, whatever
 * the handler does next. A caller of [request] that stops waiting sends `$/cancelRequest` for its own
 * request. Request ids and cancellations are scoped to this connection: each connection has its own
 * [PeerDispatcher].
 *
 * When the transport reports the end of inbound input, call [endInput] before [close] so that
 * requests that were already admitted can still be answered.
 */
public class Connection(
    private val codec: JsonRpcCodec = JsonRpcCodec.Default,
    @PublishedApi internal val json: Json = JsonRpcCodec.defaultJson,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    notificationErrorHandler: NotificationErrorHandler? = null,
) {
    @PublishedApi internal val dispatcher: Dispatcher = Dispatcher(json, notificationErrorHandler)
    internal val peer: PeerDispatcher = dispatcher.newPeer()
    private val requestIds = RequestIds()

    private sealed interface OutgoingItem {
        data class Frame(
            val message: Message,
            val onDequeued: (() -> Unit)? = null,
        ) : OutgoingItem

        data class Batch(
            val messages: List<Message>,
            val onDequeued: () -> Unit,
        ) : OutgoingItem

        data class Barrier(
            val completion: CompletableDeferred<Unit>,
        ) : OutgoingItem
    }

    private val outgoingMessages = Channel<OutgoingItem>(Channel.BUFFERED)
    private val outgoingDrain = OutgoingDrain()
    internal val pendingFlushCount: Int get() = outgoingDrain.pendingCount
    private var decoder = JsonRpcMessageDecoder(codec)
    private val connectionJob = SupervisorJob(scope.coroutineContext[Job])
    private var boundedPump: BoundedDispatchPump? = null
    private var boundedOutgoing: BoundedOutgoing? = null
    internal val retainedApplicationBytes: Int get() = boundedPump?.retainedApplicationBytes ?: 0
    private val mutableState = MutableStateFlow<ConnectionState>(ConnectionState.Open)
    private val cancellationSignal = CompletableDeferred<Unit>(connectionJob)
    private val outboundCancellation =
        OutboundCancellation(peer.pending) { notification, onDequeued ->
            val bounded = boundedOutgoing
            if (bounded != null) {
                bounded.trySend(notification, onDequeued)
            } else {
                outgoingMessages.trySend(OutgoingItem.Frame(notification, onDequeued)).isSuccess
            }
        }

    /** The first observed terminal transition and its cause, if any. */
    public val state: StateFlow<ConnectionState> = mutableState.asStateFlow()

    /**
     * Creates a connection with explicitly bounded asynchronous application dispatch.
     * In this mode [receive] returns after admission, before application handlers finish.
     * The outgoing flow requires a dedicated continuous collector: interrupting it, including
     * with `first` or `take`, terminates this opt-in connection with a failure.
     */
    public constructor(
        policy: BoundedDispatchPolicy,
        codec: JsonRpcCodec = JsonRpcCodec.Default,
        json: Json = JsonRpcCodec.defaultJson,
        scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
        notificationErrorHandler: NotificationErrorHandler? = null,
    ) : this(codec, json, scope, notificationErrorHandler) {
        installBoundedPump(policy, scope, null)
    }

    /** Creates a bounded connection with additional framed-input and application-byte limits. */
    public constructor(
        policy: BoundedDispatchPolicy,
        limits: InboundResourceLimits,
        codec: JsonRpcCodec = JsonRpcCodec.Default,
        json: Json = JsonRpcCodec.defaultJson,
        scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
        notificationErrorHandler: NotificationErrorHandler? = null,
    ) : this(codec, json, scope, notificationErrorHandler) {
        decoder = JsonRpcMessageDecoder(codec, limits)
        installBoundedPump(policy, scope, limits)
    }

    /**
     * Creates a bounded connection with fixed compact default JSON and outbound limits.
     * No custom codec is accepted by this overload. See [OutboundResourceLimits] for the
     * retained-byte boundary and the direct-collector reentrancy restriction.
     */
    public constructor(
        policy: BoundedDispatchPolicy,
        inboundLimits: InboundResourceLimits,
        outboundLimits: OutboundResourceLimits,
        scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
        notificationErrorHandler: NotificationErrorHandler? = null,
    ) : this(JsonRpcCodec.Default, JsonRpcCodec.defaultJson, scope, notificationErrorHandler) {
        decoder = JsonRpcMessageDecoder(JsonRpcCodec.Default, inboundLimits)
        boundedOutgoing =
            BoundedOutgoing(
                outboundLimits,
                fail = { cause -> boundedPump?.outputInterrupted(cause) },
                haltIngress = {
                    boundedPump?.close()
                    peer.pending.close()
                },
                parentActive = { scope.coroutineContext[Job]?.isActive != false },
                parentInactive = { transitionClosed(mutableState, boundedOutgoing, outgoingDrain) },
            )
        installBoundedPump(policy, scope, inboundLimits)
        if (!connectionJob.isActive) {
            boundedPump?.close()
            boundedOutgoing?.close(IllegalStateException("Connection closed"))
        }
    }

    private fun installBoundedPump(
        policy: BoundedDispatchPolicy,
        scope: CoroutineScope,
        limits: InboundResourceLimits?,
    ) {
        boundedPump =
            BoundedDispatchPump(
                policy = policy,
                decoder = decoder,
                peer = peer,
                scope = CoroutineScope(scope.coroutineContext + connectionJob),
                limits = limits,
                output =
                    object : DispatchOutput {
                        override val isTerminating: Boolean get() = boundedOutgoing?.isTerminating == true

                        override suspend fun send(
                            response: ResponseMessage,
                            onDequeued: () -> Unit,
                        ) {
                            val bounded = boundedOutgoing
                            if (bounded != null) {
                                bounded.send(response, onDequeued)
                            } else {
                                outgoingMessages.send(OutgoingItem.Frame(response, onDequeued))
                            }
                        }

                        override suspend fun sendBatch(
                            responses: List<ResponseMessage>,
                            onDequeued: () -> Unit,
                        ) {
                            val bounded = boundedOutgoing
                            if (bounded != null) {
                                bounded.sendBatch(responses, onDequeued)
                            } else {
                                outgoingMessages.send(OutgoingItem.Batch(responses, onDequeued))
                            }
                        }

                        override fun offer(
                            response: ResponseMessage,
                            onDequeued: (() -> Unit)?,
                        ): OfferResult =
                            boundedOutgoing?.offer(response, onDequeued)
                                ?: if (outgoingMessages.trySend(OutgoingItem.Frame(response, onDequeued)).isSuccess) {
                                    OfferResult.ACCEPTED
                                } else {
                                    OfferResult.REJECTED
                                }

                        override fun fail(cause: Throwable) {
                            if (scope.coroutineContext[Job]?.isActive == false) {
                                transitionClosed(mutableState, boundedOutgoing, outgoingDrain)
                                return
                            }
                            if (boundedOutgoing?.claimFailure(cause) == false) return
                            if (mutableState.compareAndSet(ConnectionState.Open, ConnectionState.Failed(cause))) {
                                outgoingDrain.fail(cause)
                                boundedPump?.close()
                                peer.pending.close()
                                boundedOutgoing?.close(cause)
                                outgoingMessages.cancel()
                                connectionJob.cancel()
                            }
                        }
                    },
            )
    }

    init {
        cancellationSignal.invokeOnCompletion {
            if (!connectionJob.isActive) {
                transitionClosed(mutableState, boundedOutgoing, outgoingDrain)
                boundedPump?.close()
                peer.pending.close()
                outgoingMessages.cancel()
            }
        }
    }

    private data class CompletedResponse(
        val value: JsonElement?,
    )

    /**
     * Flow of outgoing messages that need to be sent over the transport.
     * Only one collector may be active at a time.
     */
    public val outgoing: Flow<ByteArray> =
        flow {
            outgoingDrain.acquireCollector()
            try {
                val bounded = boundedOutgoing
                if (bounded != null) {
                    for (item in bounded.items) {
                        try {
                            when (item) {
                                is BoundedOutgoing.Item.Frame -> {
                                    item.onDequeued?.invoke()
                                    emit(item.bytes)
                                }
                                is BoundedOutgoing.Item.Barrier -> item.completion.complete(Unit)
                            }
                        } catch (cause: Throwable) {
                            // Fail admission before releasing this frame's capacity.
                            if (connectionJob.isActive && scope.coroutineContext[Job]?.isActive != false) {
                                bounded.resolveCollectorLoss(cause)
                                boundedPump?.outputInterrupted(cause)
                            } else {
                                transitionClosed(mutableState, boundedOutgoing, outgoingDrain)
                            }
                            val observed = mutableState.value
                            if (observed !is ConnectionState.Open) {
                                outgoingDrain.fail((observed as? ConnectionState.Failed)?.cause ?: cause)
                            }
                            throw cause
                        } finally {
                            item.lease.release()
                        }
                    }
                    return@flow
                }
                for (item in outgoingMessages) {
                    when (item) {
                        is OutgoingItem.Frame -> {
                            item.onDequeued?.invoke()
                            emit(codec.encode(item.message))
                        }
                        is OutgoingItem.Batch -> {
                            item.onDequeued()
                            emit(codec.encode(item.messages))
                        }
                        is OutgoingItem.Barrier -> item.completion.complete(Unit)
                    }
                }
            } catch (cause: Throwable) {
                // Cancellation can remove a queued item before control reaches the loop body.
                if (connectionJob.isActive && scope.coroutineContext[Job]?.isActive != false) {
                    boundedOutgoing?.resolveCollectorLoss(cause)
                    boundedPump?.outputInterrupted(cause)
                } else {
                    transitionClosed(mutableState, boundedOutgoing, outgoingDrain)
                }
                val observed = mutableState.value
                if (boundedOutgoing == null || observed !is ConnectionState.Open) {
                    outgoingDrain.fail((observed as? ConnectionState.Failed)?.cause ?: cause)
                }
                throw cause
            } finally {
                outgoingDrain.releaseCollector()
            }
        }

    /**
     * Waits until preceding frames have returned from the sequential outgoing collector.
     *
     * The collector must send each frame directly, without downstream buffering. This confirms
     * only that its call returned, not that a peer received the data. If a collector is interrupted,
     * this and later flushes fail, even if the collector was idle. A flush with no collector waits
     * until one consumes it or the connection closes.
     */
    public suspend fun flushOutgoing() {
        val completion = CompletableDeferred<Unit>(connectionJob)
        try {
            val bounded = boundedOutgoing
            if (bounded != null) {
                bounded.barrier(completion, outgoingDrain)
            } else {
                outgoingDrain.register(completion)
                select<Unit> {
                    outgoingMessages.onSend(OutgoingItem.Barrier(completion)) { }
                    completion.onAwait { }
                }
            }
            completion.await()
        } finally {
            outgoingDrain.unregister(completion)
            completion.cancel()
        }
    }

    /**
     * Register a request handler for a method.
     */
    public suspend fun onRequest(
        method: String,
        handler: RequestHandler,
    ) {
        dispatcher.onRequest(method, handler)
    }

    /**
     * Register a notification handler for a method.
     */
    public suspend fun onNotification(
        method: String,
        handler: NotificationHandler,
    ) {
        dispatcher.onNotification(method, handler)
    }

    /**
     * Register a typed request handler.
     */
    public suspend inline fun <reified P, reified R> onRequest(
        method: String,
        crossinline handler: suspend (P) -> R,
    ) {
        onRequest(
            method = method,
            paramSerializer = serializer<P>(),
            resultSerializer = serializer<R>(),
            acceptsNullParams = null is P,
        ) { params ->
            handler(params)
        }
    }

    @PublishedApi
    internal suspend fun <P, R> onRequest(
        method: String,
        paramSerializer: KSerializer<P>,
        resultSerializer: KSerializer<R>,
        acceptsNullParams: Boolean,
        handler: suspend (P) -> R,
    ) {
        dispatcher.onRequest(
            method = method,
            paramSerializer = paramSerializer,
            resultSerializer = resultSerializer,
            acceptsNullParams = acceptsNullParams,
            handler = handler,
        )
    }

    /**
     * Register a typed notification handler.
     */
    public suspend inline fun <reified P> onNotification(
        method: String,
        crossinline handler: suspend (P) -> Unit,
    ) {
        onNotification(
            method = method,
            paramSerializer = serializer<P>(),
            acceptsNullParams = null is P,
        ) { params ->
            handler(params)
        }
    }

    @PublishedApi
    internal suspend fun <P> onNotification(
        method: String,
        paramSerializer: KSerializer<P>,
        acceptsNullParams: Boolean,
        handler: suspend (P) -> Unit,
    ) {
        dispatcher.onNotification(
            method = method,
            paramSerializer = paramSerializer,
            acceptsNullParams = acceptsNullParams,
            handler = handler,
        )
    }

    /**
     * Send a request and await the response. For an outbound-limited connection, the timeout
     * includes waiting for outbound admission; legacy connections start it after enqueue.
     *
     * Cancelling the caller, or reaching [timeout], after the request was queued stops waiting for
     * it and asks the peer to cancel it with a `$/cancelRequest` notification that carries its id.
     * A response that arrives later is ignored, and a request that was already answered sends
     * nothing. The notification is queued at once, after the request and before anything that the
     * caller sends next, but it never waits: it is dropped when it would have to wait for queue
     * space or outbound admission, when eight earlier cancellations are still queued, or when the
     * connection has closed.
     */
    public suspend fun <R> request(
        method: String,
        params: JsonElement?,
        timeout: Duration = 30.seconds,
    ): JsonElement? {
        val bounded = boundedOutgoing
        if (bounded != null) {
            val result =
                withTimeoutOrNull(timeout) {
                    val (id, deferred) =
                        bounded.withProducer {
                            val requestId = RequestId.of(requestIds.next())
                            val waiter = peer.registerPendingRequest(requestId)
                            try {
                                bounded.sendRegistered(RequestMessage(id = requestId, method = method, params = params))
                                requestId to waiter
                            } catch (cause: Throwable) {
                                peer.pending.cancel(requestId, waiter)
                                throw cause
                            }
                        }
                    try {
                        CompletedResponse(deferred.await())
                    } finally {
                        outboundCancellation.abandon(id, deferred)
                    }
                }
            if (result == null) requestTimedOut(method)
            return result.value
        }

        val id = RequestId.of(requestIds.next())
        val request = RequestMessage(id = id, method = method, params = params)
        val deferred = peer.registerPendingRequest(id)
        // A send cancelled while it waits for queue space may still hand the request over; such a
        // request gets no cancellation, which is the best-effort limit of this path.
        var sent = false
        return try {
            outgoingMessages.send(OutgoingItem.Frame(request))
            sent = true
            val result = withTimeoutOrNull(timeout) { CompletedResponse(deferred.await()) }
            if (result == null) requestTimedOut(method)
            result.value
        } finally {
            if (sent) outboundCancellation.abandon(id, deferred) else peer.pending.cancel(id, deferred)
        }
    }

    /**
     * Send a typed request and await the response.
     */
    public suspend inline fun <reified P, reified R> request(
        method: String,
        params: P,
        timeout: Duration = 30.seconds,
    ): R? =
        request(
            method = method,
            params = params,
            paramSerializer = serializer<P>(),
            resultSerializer = serializer<R>(),
            timeout = timeout,
        )

    @PublishedApi
    internal suspend fun <P, R> request(
        method: String,
        params: P,
        paramSerializer: KSerializer<P>,
        resultSerializer: KSerializer<R>,
        timeout: Duration,
    ): R? {
        val jsonParams =
            if (params != null) {
                json.encodeToJsonElement(paramSerializer, params)
            } else {
                null
            }
        val result = request<R>(method, jsonParams, timeout)
        return if (result != null) {
            json.decodeFromJsonElement(resultSerializer, result)
        } else {
            null
        }
    }

    /**
     * Send a notification (no response expected).
     */
    public suspend fun notify(
        method: String,
        params: JsonElement?,
    ) {
        val notification = NotificationMessage(method = method, params = params)
        val bounded = boundedOutgoing
        if (bounded != null) bounded.send(notification) else outgoingMessages.send(OutgoingItem.Frame(notification))
    }

    /**
     * Send a typed notification.
     */
    public suspend inline fun <reified P> notify(
        method: String,
        params: P,
    ) {
        notify(
            method = method,
            params = params,
            paramSerializer = serializer<P>(),
        )
    }

    @PublishedApi
    internal suspend fun <P> notify(
        method: String,
        params: P,
        paramSerializer: KSerializer<P>,
    ) {
        val jsonParams =
            if (params != null) {
                json.encodeToJsonElement(paramSerializer, params)
            } else {
                null
            }
        notify(method, jsonParams)
    }

    /**
     * Feed incoming data from the transport.
     * This will decode messages and dispatch them to handlers.
     * In bounded mode, cancellation after admission starts fails the connection because the
     * remainder of the supplied data cannot safely be replayed. Cancellation while waiting
     * for admission does not consume data or fail the connection.
     */
    public suspend fun receive(data: ByteArray): Unit =
        coroutineScope {
            val receiveJob = coroutineContext[Job]!!
            val completion = cancellationSignal.invokeOnCompletion { receiveJob.cancel() }
            try {
                ensureActive()
                val pump = boundedPump
                if (pump != null) {
                    pump.receive(data)
                    return@coroutineScope
                }
                // TODO(#49): Bound ingress and support ordered full-duplex dispatch and recovery.
                for (payload in decoder.feed(data)) {
                    when (payload) {
                        is Payload.Single -> peer.dispatch(payload.message)?.let { outgoingMessages.send(OutgoingItem.Frame(it)) }
                        is Payload.Batch -> {
                            val responses = peer.dispatch(payload)
                            if (responses.isNotEmpty()) outgoingMessages.send(OutgoingItem.Batch(responses) {})
                        }
                        is Payload.Invalid -> outgoingMessages.send(OutgoingItem.Frame(payload.response))
                    }
                }
            } finally {
                completion.dispose()
            }
        }

    /**
     * Tells the connection that the transport delivered its last inbound byte, and waits for the
     * inbound work that was already admitted.
     *
     * In bounded mode, admitted requests keep running after the end of input. This waits until each
     * of them has been answered and its response dequeued by the outgoing collector, and until queued
     * notifications and batches have run, so a peer that closes its output right after its last
     * request still gets the response. When [BoundedDispatchPolicy.drainTimeout] elapses first, it
     * cancels the handlers that remain, answers each request that is still unanswered with a Request
     * Cancelled error (-32800) where the outgoing queue accepts it, and returns. It returns at once
     * when the connection is closed or failed, for example because the outgoing collector stopped,
     * since no response could be written. Later [receive] calls throw [IllegalStateException].
     *
     * The connection stays open either way, so outbound requests and notifications still work;
     * [close] it when done. In the default mode [receive] has already run every handler, so this
     * returns at once and has no effect.
     */
    public suspend fun endInput() {
        boundedPump?.endInput()
    }

    /**
     * Feed incoming data from the transport.
     */
    public suspend fun receive(data: String) {
        receive(data.encodeToByteArray())
    }

    /**
     * Close the connection and cancel all pending operations.
     */
    public fun close() {
        transitionClosed(mutableState, boundedOutgoing, outgoingDrain)
        boundedPump?.close()
        peer.pending.close()
        outgoingMessages.cancel()
        connectionJob.cancel()
    }
}

private class RequestIds {
    private var value = 0L
    private val mutex = Mutex()

    suspend fun next(): Long = mutex.withLock { ++value }
}

private fun transitionClosed(
    state: MutableStateFlow<ConnectionState>,
    outgoing: BoundedOutgoing?,
    drain: OutgoingDrain,
) {
    val claimed = outgoing?.closeNormally()
    val terminal = if (claimed == null) ConnectionState.Closed else ConnectionState.Failed(claimed)
    if (state.compareAndSet(ConnectionState.Open, terminal) && claimed != null) drain.fail(claimed)
}

private fun requestTimedOut(method: String): Nothing = throw JsonRpcException(ResponseError.REQUEST_CANCELLED, "Request timed out: $method")
