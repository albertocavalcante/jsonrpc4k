package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.KSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
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

/**
 * Dispatches incoming JSON-RPC messages to registered handlers.
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
    private val pendingRequests = mutableMapOf<RequestId, CompletableDeferred<JsonElement?>>()

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
     * Returns a response message if the input was a request, null otherwise.
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
     * Register a pending request that expects a response.
     * Returns a Deferred that will complete when the response arrives.
     */
    public suspend fun registerPendingRequest(id: RequestId): CompletableDeferred<JsonElement?> {
        val deferred = CompletableDeferred<JsonElement?>()
        mutex.withLock {
            pendingRequests[id] = deferred
        }
        return deferred
    }

    /**
     * Cancel all pending requests. Called during connection shutdown.
     */
    public fun cancelAllPendingRequests() {
        pendingRequests.values.forEach { it.cancel() }
        pendingRequests.clear()
    }

    /**
     * Cancel a pending request.
     */
    public suspend fun cancelPendingRequest(id: RequestId) {
        mutex
            .withLock {
                pendingRequests.remove(id)
            }?.cancel()
    }

    private suspend fun handleRequest(request: RequestMessage): ResponseMessage {
        val handler =
            mutex.withLock {
                requestHandlers[request.method]
            } ?: return ResponseMessage.error(request.id, ResponseError.methodNotFound(request.method))

        return try {
            val result = handler.handle(request.params)
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
        // Handle cancellation specially
        if (notification.method == CANCEL_REQUEST_METHOD) {
            handleCancelRequest(notification.params)
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

    private suspend fun handleCancelRequest(params: JsonElement?) {
        val idElement = (params as? JsonObject)?.get("id") ?: return
        val requestId =
            when {
                idElement is JsonPrimitive && idElement.isString ->
                    RequestId.of(idElement.content)
                idElement is JsonPrimitive ->
                    RequestId.of(idElement.content.toLongOrNull() ?: return)
                else -> return
            }
        cancelPendingRequest(requestId)
    }

    private suspend fun handleResponse(response: ResponseMessage) {
        val id = response.id ?: return
        val deferred =
            mutex.withLock {
                pendingRequests.remove(id)
            } ?: return

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
        /** Standard cancellation method name for JSON-RPC base protocol. */
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
