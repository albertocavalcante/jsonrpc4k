package io.jsonrpc4k.core

import kotlinx.collections.immutable.PersistentMap
import kotlinx.collections.immutable.persistentHashMapOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.encodeToJsonElement
import kotlinx.serialization.serializer
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * A handler for JSON-RPC requests.
 */
public fun interface RequestHandler {
    /**
     * Handle a request and return a result.
     * @param params The request parameters (may be null)
     * @return The result to send back (may be null for void methods)
     * @throws Exception If an error occurs, it will be converted to a ResponseError
     */
    public suspend fun handle(params: JsonElement?): JsonElement?
}

/**
 * A handler for JSON-RPC notifications.
 */
public fun interface NotificationHandler {
    /**
     * Handle a notification.
     * @param params The notification parameters (may be null)
     */
    public suspend fun handle(params: JsonElement?)
}

/**
 * Callback invoked when a notification handler throws an exception.
 *
 * Since JSON-RPC notifications have no response, errors cannot be reported
 * to the sender. This callback allows the host to log or handle such errors.
 *
 * @param method The notification method that failed.
 * @param exception The exception thrown by the handler.
 */
public fun interface NotificationErrorHandler {
    public fun onError(
        method: String,
        exception: Exception,
    )
}

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

/**
 * Dispatches incoming JSON-RPC messages to registered handlers.
 *
 * Request ids are scoped to one peer, so a dispatcher serves a single connection. An inbound
 * `$/cancelRequest` cancels the running handler of the request that it names, and [dispatch] then
 * answers that request with a Request Cancelled error (-32800), unless the handler ends with its
 * own [JsonRpcException]. Cancellation is cooperative and reaches a handler only while it runs,
 * so it needs messages to be dispatched concurrently. It never completes an outbound waiter that
 * was registered with the same id.
 *
 * @param json The JSON configuration used for serialization/deserialization.
 * @param notificationErrorHandler Optional handler invoked when a notification handler throws.
 *   By default, notification handler exceptions are silently ignored per the JSON-RPC spec
 *   (notifications have no response). Provide a handler to log or react to these errors.
 */
public class Dispatcher(
    @PublishedApi internal val json: Json = JsonRpcCodec.defaultJson,
    private val notificationErrorHandler: NotificationErrorHandler? = null,
) {
    private val mutex = Mutex()
    private val requestHandlers = mutableMapOf<String, RequestHandler>()
    private val notificationHandlers = mutableMapOf<String, NotificationHandler>()
    internal val pending = PendingRequests()
    internal val inbound = InboundRequests()

    /**
     * Register a request handler for a method.
     */
    public suspend fun onRequest(
        method: String,
        handler: RequestHandler,
    ) {
        mutex.withLock {
            requestHandlers[method] = handler
        }
    }

    /**
     * Register a notification handler for a method.
     */
    public suspend fun onNotification(
        method: String,
        handler: NotificationHandler,
    ) {
        mutex.withLock {
            notificationHandlers[method] = handler
        }
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
        onRequest(method) { params ->
            val typedParams: P =
                if (params != null) {
                    json.decodeFromJsonElement(paramSerializer, params)
                } else if (acceptsNullParams) {
                    @Suppress("UNCHECKED_CAST")
                    null as P
                } else {
                    throw JsonRpcException(ResponseError.INVALID_PARAMS, "Missing required params for method")
                }
            val result = handler(typedParams)
            if (result != null) {
                json.encodeToJsonElement(resultSerializer, result)
            } else {
                null
            }
        }
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
        onNotification(method) { params ->
            val typedParams: P =
                if (params != null) {
                    json.decodeFromJsonElement(paramSerializer, params)
                } else if (acceptsNullParams) {
                    @Suppress("UNCHECKED_CAST")
                    null as P
                } else {
                    throw JsonRpcException(ResponseError.INVALID_PARAMS, "Missing required params for notification")
                }
            handler(typedParams)
        }
    }

    /**
     * Dispatch an incoming message.
     * Returns a response message if the input was a request, null otherwise. A request's handler
     * runs until it returns, or until a `$/cancelRequest` for its id, dispatched concurrently,
     * cancels it.
     */
    public suspend fun dispatch(message: Message): ResponseMessage? =
        when (message) {
            is RequestMessage -> handleRequest(message)
            is NotificationMessage -> {
                handleNotification(message)
                null
            }
            is ResponseMessage -> {
                handleResponse(message)
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
        batch.entries.mapNotNull { entry -> if (entry == null) Payload.Invalid.INVALID_REQUEST.response else dispatch(entry) }

    /**
     * Register a pending request that expects a response.
     * Returns a Deferred that will complete when the response arrives.
     */
    public suspend fun registerPendingRequest(id: RequestId): CompletableDeferred<JsonElement?> = pending.register(id)

    /**
     * Cancel all pending requests. Called during connection shutdown.
     */
    public fun cancelAllPendingRequests() {
        pending.cancelAll()
    }

    /**
     * Cancel a pending request.
     */
    public suspend fun cancelPendingRequest(id: RequestId) {
        pending.remove(id)?.cancel()
    }

    private suspend fun handleRequest(request: RequestMessage): ResponseMessage {
        val handler =
            mutex.withLock {
                requestHandlers[request.method]
            } ?: return ResponseMessage.error(request.id, ResponseError.methodNotFound(request.method))

        return try {
            val result = inbound.runCancellable(request.id) { handler.handle(request.params) }
            ResponseMessage.success(request.id, result)
        } catch (e: CancellationException) {
            throw e
        } catch (e: JsonRpcException) {
            ResponseMessage.error(request.id, e.toResponseError())
        } catch (e: Exception) {
            // Don't expose exception details for security (SEC-001)
            ResponseMessage.error(
                request.id,
                ResponseError.internalError("Internal error"),
            )
        }
    }

    private suspend fun handleNotification(notification: NotificationMessage) {
        if (notification.method == CANCEL_REQUEST_METHOD) {
            // Only inbound handlers are cancelled, so an outbound waiter with the same id keeps waiting.
            cancelledRequestId(notification.params)?.let(inbound::cancel)
            return
        }
        val handler =
            mutex.withLock {
                notificationHandlers[notification.method]
            } ?: return
        try {
            handler.handle(notification.params)
        } catch (e: CancellationException) {
            throw e
        } catch (e: Exception) {
            notificationErrorHandler?.onError(notification.method, e)
        }
    }

    private suspend fun handleResponse(response: ResponseMessage) {
        val id = response.id ?: return
        val deferred = pending.remove(id) ?: return

        if (response.error != null) {
            deferred.completeExceptionally(
                JsonRpcException(
                    response.error.code,
                    response.error.message,
                    response.error.data,
                ),
            )
        } else {
            deferred.complete(response.result)
        }
    }

    public companion object {
        /** Cancellation notification method used by LSP. */
        public const val CANCEL_REQUEST_METHOD: String = "$/cancelRequest"
    }
}

/**
 * Exception representing a JSON-RPC error that can be converted to a [ResponseError].
 *
 * Thrown by request handlers to signal a specific JSON-RPC error code back to the client.
 */
public class JsonRpcException(
    /** The JSON-RPC error code (see [ResponseError] companion constants). */
    public val code: Int,
    override val message: String,
    /** Optional structured data providing additional information about the error. */
    public val data: JsonElement? = null,
) : Exception(message) {
    /** Converts this exception to a [ResponseError] suitable for a JSON-RPC response. */
    public fun toResponseError(): ResponseError = ResponseError(code, message, data)

    public companion object {
        /**
         * Creates a [JsonRpcException] for an unknown method.
         * @param method The method name that was not found.
         */
        public fun methodNotFound(method: String): JsonRpcException =
            JsonRpcException(ResponseError.METHOD_NOT_FOUND, "Method not found: $method")

        /**
         * Creates a [JsonRpcException] for invalid parameters.
         * @param message Description of the parameter validation failure.
         */
        public fun invalidParams(message: String): JsonRpcException = JsonRpcException(ResponseError.INVALID_PARAMS, message)

        /**
         * Creates a [JsonRpcException] for an internal server error.
         * @param message Description of the internal error.
         */
        public fun internalError(message: String): JsonRpcException = JsonRpcException(ResponseError.INTERNAL_ERROR, message)
    }
}

/**
 * Utility to await a response with timeout.
 */
public suspend fun CompletableDeferred<JsonElement?>.awaitWithTimeout(timeout: Duration = 30.seconds): JsonElement? =
    withTimeout(timeout) {
        await()
    }
