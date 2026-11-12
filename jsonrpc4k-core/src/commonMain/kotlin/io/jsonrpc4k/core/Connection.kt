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
 */
public class Connection(
    private val codec: JsonRpcCodec = JsonRpcCodec.Default,
    @PublishedApi internal val json: Json = JsonRpcCodec.defaultJson,
    private val scope: CoroutineScope = CoroutineScope(Dispatchers.Default + SupervisorJob()),
    notificationErrorHandler: NotificationErrorHandler? = null,
) {
    @PublishedApi internal val dispatcher: Dispatcher = Dispatcher(json, notificationErrorHandler)
    private var requestIdValue = 0L
    private val requestIdMutex = Mutex()

    private sealed interface OutgoingItem {
        data class Frame(
            val message: Message,
            val onDequeued: (() -> Unit)? = null,
        ) : OutgoingItem

        data class Barrier(
            val completion: CompletableDeferred<Unit>,
        ) : OutgoingItem
    }

    private val outgoingMessages = Channel<OutgoingItem>(Channel.BUFFERED)
    private val outgoingDrain = OutgoingDrain()
    internal val pendingFlushCount: Int get() = outgoingDrain.pendingCount
    private val decoder = JsonRpcMessageDecoder(codec)
    private val connectionJob = SupervisorJob(scope.coroutineContext[Job])
    private var boundedPump: BoundedDispatchPump? = null
    private val mutableState = MutableStateFlow<ConnectionState>(ConnectionState.Open)
    private val cancellationSignal = CompletableDeferred<Unit>(connectionJob)

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
        boundedPump =
            BoundedDispatchPump(
                policy = policy,
                decoder = decoder,
                dispatcher = dispatcher,
                scope = CoroutineScope(scope.coroutineContext + connectionJob),
                output =
                    object : DispatchOutput {
                        override suspend fun send(
                            response: ResponseMessage,
                            onDequeued: () -> Unit,
                        ) {
                            outgoingMessages.send(OutgoingItem.Frame(response, onDequeued))
                        }

                        override fun offer(
                            response: ResponseMessage,
                            onDequeued: (() -> Unit)?,
                        ): Boolean = outgoingMessages.trySend(OutgoingItem.Frame(response, onDequeued)).isSuccess

                        override fun fail(cause: Throwable) {
                            if (scope.coroutineContext[Job]?.isActive == false) {
                                mutableState.compareAndSet(ConnectionState.Open, ConnectionState.Closed)
                                return
                            }
                            if (mutableState.compareAndSet(ConnectionState.Open, ConnectionState.Failed(cause))) {
                                boundedPump?.close()
                                dispatcher.pending.close()
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
                mutableState.compareAndSet(ConnectionState.Open, ConnectionState.Closed)
                boundedPump?.close()
                dispatcher.pending.close()
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
                for (item in outgoingMessages) {
                    when (item) {
                        is OutgoingItem.Frame -> {
                            item.onDequeued?.invoke()
                            emit(codec.encode(item.message))
                        }
                        is OutgoingItem.Barrier -> item.completion.complete(Unit)
                    }
                }
            } catch (cause: Throwable) {
                // Cancellation can remove a queued item before control reaches the loop body.
                outgoingDrain.fail(cause)
                if (connectionJob.isActive && scope.coroutineContext[Job]?.isActive != false) {
                    boundedPump?.outputInterrupted(cause)
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
            outgoingDrain.register(completion)
            select<Unit> {
                outgoingMessages.onSend(OutgoingItem.Barrier(completion)) { }
                completion.onAwait { }
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
     * Send a request and await the response.
     */
    private suspend fun nextRequestId(): Long = requestIdMutex.withLock { ++requestIdValue }

    public suspend fun <R> request(
        method: String,
        params: JsonElement?,
        timeout: Duration = 30.seconds,
    ): JsonElement? {
        val id = RequestId.of(nextRequestId())
        val request = RequestMessage(id = id, method = method, params = params)

        val deferred = dispatcher.registerPendingRequest(id)
        return try {
            outgoingMessages.send(OutgoingItem.Frame(request))
            val result = withTimeoutOrNull(timeout) { CompletedResponse(deferred.await()) }
            if (result == null) {
                throw JsonRpcException(ResponseError.REQUEST_CANCELLED, "Request timed out: $method")
            }
            result.value
        } finally {
            dispatcher.pending.cancel(id, deferred)
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
        outgoingMessages.send(OutgoingItem.Frame(notification))
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
                val messages = decoder.feed(data)
                for (message in messages) {
                    val response = dispatcher.dispatch(message)
                    if (response != null) {
                        outgoingMessages.send(OutgoingItem.Frame(response))
                    }
                }
            } finally {
                completion.dispose()
            }
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
        mutableState.compareAndSet(ConnectionState.Open, ConnectionState.Closed)
        boundedPump?.close()
        dispatcher.pending.close()
        outgoingMessages.cancel()
        connectionJob.cancel()
    }
}
