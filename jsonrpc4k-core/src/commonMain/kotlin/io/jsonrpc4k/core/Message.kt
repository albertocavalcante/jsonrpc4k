package io.jsonrpc4k.core

import kotlinx.serialization.DeserializationStrategy
import kotlinx.serialization.EncodeDefault
import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.descriptors.buildClassSerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.JsonContentPolymorphicSerializer
import kotlinx.serialization.json.JsonDecoder
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonEncoder
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive

/**
 * JSON-RPC 2.0 protocol version constant.
 */
public const val JSONRPC_VERSION: String = "2.0"

/**
 * Request ID can be either a number or a string.
 * We use a sealed interface for type safety.
 */
@Serializable(with = RequestIdSerializer::class)
public sealed interface RequestId {
    @Serializable
    public data class NumberId(
        public val value: Long,
    ) : RequestId {
        override fun toString(): String = value.toString()
    }

    @Serializable
    public data class StringId(
        public val value: String,
    ) : RequestId {
        override fun toString(): String = value
    }

    public companion object {
        /** Creates a numeric request ID. */
        public fun of(value: Long): RequestId = NumberId(value)

        /** Creates a string request ID. */
        public fun of(value: String): RequestId = StringId(value)
    }
}

/**
 * Custom serializer for RequestId to handle both number and string IDs.
 * Serializes NumberId as a primitive number and StringId as a primitive string.
 */
public object RequestIdSerializer : KSerializer<RequestId> {
    override val descriptor: SerialDescriptor =
        buildClassSerialDescriptor("RequestId")

    override fun serialize(
        encoder: Encoder,
        value: RequestId,
    ) {
        val jsonEncoder = encoder as JsonEncoder
        when (value) {
            is RequestId.NumberId -> jsonEncoder.encodeJsonElement(JsonPrimitive(value.value))
            is RequestId.StringId -> jsonEncoder.encodeJsonElement(JsonPrimitive(value.value))
        }
    }

    override fun deserialize(decoder: Decoder): RequestId {
        val jsonDecoder = decoder as JsonDecoder
        val element = jsonDecoder.decodeJsonElement()
        return when {
            element.jsonPrimitive.isString -> RequestId.StringId(element.jsonPrimitive.content)
            else -> RequestId.NumberId(element.jsonPrimitive.content.toLong())
        }
    }
}

/**
 * Base sealed interface for all JSON-RPC 2.0 messages.
 */
@Serializable(with = MessageSerializer::class)
public sealed interface Message {
    public val jsonrpc: String get() = JSONRPC_VERSION
}

/**
 * A JSON-RPC 2.0 request message.
 */
@OptIn(ExperimentalSerializationApi::class)
@Serializable
public data class RequestMessage(
    /**
     * The request id.
     */
    val id: RequestId,
    /**
     * The method to be invoked.
     */
    val method: String,
    /**
     * The method's params.
     */
    val params: JsonElement? = null,
    /**
     * The JSON-RPC protocol version.
     */
    @EncodeDefault
    override val jsonrpc: String = JSONRPC_VERSION,
) : Message

/**
 * A JSON-RPC 2.0 response message.
 */
@OptIn(ExperimentalSerializationApi::class)
@Serializable
public data class ResponseMessage(
    /**
     * The request id.
     */
    val id: RequestId?,
    /**
     * The result of a request. This member is REQUIRED on success.
     * This member MUST NOT exist if there was an error invoking the method.
     */
    val result: JsonElement? = null,
    /**
     * The error object in case a request fails.
     */
    val error: ResponseError? = null,
    /**
     * The JSON-RPC protocol version.
     */
    @EncodeDefault
    override val jsonrpc: String = JSONRPC_VERSION,
) : Message {
    public companion object {
        /**
         * Creates a successful response message.
         * @param id The request ID this response corresponds to.
         * @param result The result payload, or `null` for void methods.
         */
        public fun success(
            id: RequestId,
            result: JsonElement?,
        ): ResponseMessage = ResponseMessage(id = id, result = result)

        /**
         * Creates an error response message.
         * @param id The request ID this response corresponds to, or `null` for parse errors.
         * @param error The error details.
         */
        public fun error(
            id: RequestId?,
            error: ResponseError,
        ): ResponseMessage = ResponseMessage(id = id, error = error)
    }
}

/**
 * A JSON-RPC 2.0 notification message.
 * A notification is a request without an id - no response is expected.
 */
@OptIn(ExperimentalSerializationApi::class)
@Serializable
public data class NotificationMessage(
    /**
     * The method to be invoked.
     */
    val method: String,
    /**
     * The notification's params.
     */
    val params: JsonElement? = null,
    /**
     * The JSON-RPC protocol version.
     */
    @EncodeDefault
    override val jsonrpc: String = JSONRPC_VERSION,
) : Message

/**
 * JSON-RPC 2.0 error object.
 */
@Serializable
public data class ResponseError(
    /**
     * A number indicating the error type that occurred.
     */
    val code: Int,
    /**
     * A string providing a short description of the error.
     */
    val message: String,
    /**
     * A primitive or structured value that contains additional information about the error.
     */
    val data: JsonElement? = null,
) {
    public companion object {
        // -- JSON-RPC 2.0 defined error codes --

        /** JSON-RPC parse error (-32700). The JSON sent is not valid. */
        public const val PARSE_ERROR: Int = -32700

        /** JSON-RPC invalid request (-32600). The JSON sent is not a valid request object. */
        public const val INVALID_REQUEST: Int = -32600

        /** JSON-RPC method not found (-32601). The method does not exist or is not available. */
        public const val METHOD_NOT_FOUND: Int = -32601

        /** JSON-RPC invalid params (-32602). Invalid method parameters. */
        public const val INVALID_PARAMS: Int = -32602

        /** JSON-RPC internal error (-32603). An internal JSON-RPC error. */
        public const val INTERNAL_ERROR: Int = -32603

        // -- JSON-RPC server error codes (reserved range -32000 to -32099) --

        /** Server not initialized (-32002). The server has not been initialized. */
        public const val SERVER_NOT_INITIALIZED: Int = -32002

        /** Unknown error code (-32001). Catch-all for errors not covered by other codes. */
        public const val UNKNOWN_ERROR_CODE: Int = -32001

        // -- Extended error codes (reserved range -32800 to -32899) --

        /** Request failed (-32803). A generic request failure when no more specific code applies. */
        public const val REQUEST_FAILED: Int = -32803

        /** Server cancelled (-32802). The server detected that the result would be stale. */
        public const val SERVER_CANCELLED: Int = -32802

        /** Content modified (-32801). The content the request operates on was modified. */
        public const val CONTENT_MODIFIED: Int = -32801

        /** Request cancelled (-32800). The client cancelled a request and the server has detected it. */
        public const val REQUEST_CANCELLED: Int = -32800

        /**
         * Creates a parse error response.
         * @param message Description of the parse error.
         * @param data Optional additional error data.
         */
        public fun parseError(
            message: String,
            data: JsonElement? = null,
        ): ResponseError = ResponseError(PARSE_ERROR, message, data)

        /**
         * Creates an invalid request error response.
         * @param message Description of why the request is invalid.
         * @param data Optional additional error data.
         */
        public fun invalidRequest(
            message: String,
            data: JsonElement? = null,
        ): ResponseError = ResponseError(INVALID_REQUEST, message, data)

        /**
         * Creates a method-not-found error response.
         * @param method The method name that was not found.
         */
        public fun methodNotFound(method: String): ResponseError = ResponseError(METHOD_NOT_FOUND, "Method not found: $method")

        /**
         * Creates an invalid params error response.
         * @param message Description of the parameter validation failure.
         * @param data Optional additional error data.
         */
        public fun invalidParams(
            message: String,
            data: JsonElement? = null,
        ): ResponseError = ResponseError(INVALID_PARAMS, message, data)

        /**
         * Creates an internal error response.
         * @param message Description of the internal error.
         * @param data Optional additional error data.
         */
        public fun internalError(
            message: String,
            data: JsonElement? = null,
        ): ResponseError = ResponseError(INTERNAL_ERROR, message, data)

        /** Creates a server-not-initialized error response. */
        public fun serverNotInitialized(): ResponseError = ResponseError(SERVER_NOT_INITIALIZED, "Server not initialized")

        /** Creates a request-cancelled error response. */
        public fun requestCancelled(): ResponseError = ResponseError(REQUEST_CANCELLED, "Request cancelled")

        /** Creates a content-modified error response. */
        public fun contentModified(): ResponseError = ResponseError(CONTENT_MODIFIED, "Content modified")
    }
}

/**
 * Polymorphic serializer for Message that determines the concrete type
 * based on the presence of id/method/result/error fields.
 */
public object MessageSerializer : JsonContentPolymorphicSerializer<Message>(Message::class) {
    override fun selectDeserializer(element: JsonElement): DeserializationStrategy<Message> {
        val jsonObject = element.jsonObject

        // Validate jsonrpc version
        val jsonrpcVersion = jsonObject["jsonrpc"]?.jsonPrimitive?.content
        require(jsonrpcVersion != null) { "Missing required jsonrpc field" }
        require(jsonrpcVersion == JSONRPC_VERSION) { "Invalid jsonrpc version: $jsonrpcVersion (expected $JSONRPC_VERSION)" }

        // Validate that response doesn't have both result and error
        require(!("result" in jsonObject && "error" in jsonObject)) {
            "Response cannot have both result and error"
        }

        return when {
            // Has "id" field
            "id" in jsonObject -> {
                // Has "method" field -> Request
                if ("method" in jsonObject) {
                    RequestMessage.serializer()
                } else {
                    // Has "result" or "error" field -> Response
                    ResponseMessage.serializer()
                }
            }
            // No "id" but has "method" -> Notification
            "method" in jsonObject -> NotificationMessage.serializer()
            // Response with null id (error response to notification)
            "result" in jsonObject || "error" in jsonObject -> ResponseMessage.serializer()
            else -> throw IllegalArgumentException("Cannot determine message type from: $element")
        }
    }
}
