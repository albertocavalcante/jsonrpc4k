package io.jsonrpc4k.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
    private val outgoingMessages = Channel<Message>(Channel.BUFFERED)
    private val decoder = JsonRpcMessageDecoder(codec)

    /**
     * Flow of outgoing messages that need to be sent over the transport.
     */
    public val outgoing: Flow<ByteArray> =
        outgoingMessages
            .receiveAsFlow()
            .map { codec.encode(it) }

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
        outgoingMessages.send(request)

        return try {
            deferred.awaitWithTimeout(timeout)
        } catch (e: TimeoutCancellationException) {
            dispatcher.cancelPendingRequest(id)
            throw JsonRpcException(ResponseError.REQUEST_CANCELLED, "Request timed out: $method")
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
        outgoingMessages.send(notification)
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
     */
    public suspend fun receive(data: ByteArray) {
        val messages = decoder.feed(data)
        for (message in messages) {
            val response = dispatcher.dispatch(message)
            if (response != null) {
                outgoingMessages.send(response)
            }
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
        dispatcher.cancelAllPendingRequests()
        outgoingMessages.close()
        scope.cancel()
    }
}
