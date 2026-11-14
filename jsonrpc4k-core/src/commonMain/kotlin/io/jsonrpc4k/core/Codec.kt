package io.jsonrpc4k.core

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement

/**
 * JSON-RPC message codec for encoding/decoding messages with Content-Length headers.
 *
 * Messages are framed with HTTP-like headers:
 * ```
 * Content-Length: <length>\r\n
 * \r\n
 * <JSON content>
 * ```
 */
public class JsonRpcCodec(
    private val json: Json = defaultJson,
) {
    @OptIn(ExperimentalSerializationApi::class)
    internal val allowsComments: Boolean get() = json.configuration.allowComments

    /**
     * Encode a message to wire format with Content-Length header.
     * Lone UTF-16 units in quoted strings use JSON escapes to preserve their value on the wire.
     */
    public fun encode(message: Message): ByteArray = frame(encodeToJson(message))

    /**
     * Encode a JSON-RPC 2.0 batch: one frame whose content is a JSON array of [messages].
     */
    public fun encode(messages: List<Message>): ByteArray = frame(encodeToJson(messages))

    /**
     * Encode a message to JSON string (without headers).
     */
    public fun encodeToJson(message: Message): String = json.encodeToString(Message.serializer(), message)

    /**
     * Encode a JSON-RPC 2.0 batch to a JSON array string (without headers). A batch is never empty.
     */
    public fun encodeToJson(messages: List<Message>): String {
        require(messages.isNotEmpty()) { "A batch must contain at least one message" }
        return json.encodeToString(ListSerializer(Message.serializer()), messages)
    }

    /**
     * Decode a JSON string to a single Message, throwing if it is not one valid message object.
     */
    public fun decodeFromJson(jsonString: String): Message = json.decodeFromString(Message.serializer(), jsonString)

    /**
     * Decode one frame body into a [Payload] (JSON-RPC 2.0 sections 5.1 and 6). Malformed input is a
     * value, not an exception: text that is not JSON is [Payload.Invalid.PARSE_ERROR], and an empty
     * array or a value that is not a message object is [Payload.Invalid.INVALID_REQUEST]. A batch
     * keeps each invalid element as a `null` entry.
     */
    public fun decodePayload(jsonString: String): Payload {
        val element =
            try {
                json.parseToJsonElement(jsonString)
            } catch (_: SerializationException) {
                return Payload.Invalid.PARSE_ERROR
            }
        return when {
            element !is JsonArray -> decodeMessageOrNull(element)?.let { Payload.Single(it) } ?: Payload.Invalid.INVALID_REQUEST
            element.isEmpty() -> Payload.Invalid.INVALID_REQUEST
            else -> Payload.Batch(element.map { decodeMessageOrNull(it) })
        }
    }

    private fun decodeMessageOrNull(element: JsonElement): Message? =
        try {
            json.decodeFromJsonElement(Message.serializer(), element)
        } catch (_: IllegalArgumentException) {
            // SerializationException is an IllegalArgumentException, as are the envelope checks.
            null
        }

    private fun frame(content: String): ByteArray {
        val contentBytes = content.escapeLoneJsonSurrogates(allowComments = allowsComments).encodeToByteArray()
        val header = "Content-Length: ${contentBytes.size}\r\n\r\n"
        return header.encodeToByteArray() + contentBytes
    }

    /**
     * Parse Content-Length header from a header line.
     * Returns null if the line is not a Content-Length header or if the value is invalid.
     * Only accepts non-negative integers (digits only, no signs).
     */
    public fun parseContentLength(headerLine: String): Int? {
        val trimmed = headerLine.trim()
        if (!trimmed.startsWith(CONTENT_LENGTH_HEADER, ignoreCase = true)) {
            return null
        }
        val value = trimmed.substringAfter(':').trim()
        // Only accept digits (SEC-003: reject negative values and non-numeric input)
        if (value.isEmpty() || !value.all { it.isDigit() }) {
            return null
        }
        return value.toIntOrNull()?.takeIf { it <= MAX_CONTENT_LENGTH }
    }

    public companion object {
        public const val CONTENT_LENGTH_HEADER: String = "Content-Length:"
        public const val MAX_CONTENT_LENGTH: Int = 100 * 1024 * 1024 // 100 MB

        /** Maximum header bytes, including the four-byte CRLF CRLF delimiter. */
        public const val MAX_HEADER_LENGTH: Int = 16 * 1024
        public const val HEADER_DELIMITER: String = "\r\n\r\n"
        public const val LINE_DELIMITER: String = "\r\n"

        /**
         * Default JSON configuration for JSON-RPC.
         */
        public val defaultJson: Json =
            Json {
                ignoreUnknownKeys = true
                encodeDefaults = false
                isLenient = true
                coerceInputValues = true
            }

        /**
         * Shared instance with default configuration.
         */
        public val Default: JsonRpcCodec = JsonRpcCodec()
    }
}

/**
 * A streaming decoder for JSON-RPC payloads.
 * Handles partial reads and buffering.
 *
 * Note: Content-Length specifies the number of bytes, not characters.
 * This decoder maintains a byte buffer to correctly handle multi-byte UTF-8 characters.
 * Header and content limits apply per frame, including when a feed contains multiple frames.
 * A framing or resource-limit failure is terminal until [reset] starts a new stream. A well-framed
 * body that is not valid JSON-RPC is not a failure: it decodes to [Payload.Invalid] and the stream
 * continues, as each frame is decoded by [JsonRpcCodec.decodePayload].
 */
public class JsonRpcMessageDecoder(
    private val codec: JsonRpcCodec = JsonRpcCodec.Default,
) {
    private var limits: InboundResourceLimits? = null
    private var lastDecodedLength = 0

    internal constructor(codec: JsonRpcCodec, limits: InboundResourceLimits) : this(codec) {
        this.limits = limits
    }

    private var byteBuffer = ByteArray(0)
    private var byteBufferSize = 0
    private var expectedContentLength: Int? = null
    private var headersParsed = false
    private var headerSearchOffset = 0
    private var failure: Exception? = null

    /**
     * Feed data into the decoder.
     * Returns the complete payloads that were decoded, one per frame.
     * A framing failure is terminal until [reset] starts a new stream.
     */
    public fun feed(data: String): List<Payload> = feed(data.encodeToByteArray())

    /**
     * Feed data into the decoder.
     * Returns the complete payloads that were decoded, one per frame.
     */
    public fun feed(data: ByteArray): List<Payload> {
        failure?.let { throw it }
        try {
            val payloads = mutableListOf<Payload>()
            var offset = 0
            while (offset < data.size) {
                offset += appendChunk(data, offset)
                drainPayloads(payloads)
            }
            return payloads
        } catch (cause: Exception) {
            reset()
            failure = cause
            throw cause
        }
    }

    /** Processes each completed frame before decoding the next one. */
    internal suspend fun feedEach(
        data: ByteArray,
        accept: suspend (Payload, Int) -> Unit,
    ) {
        failure?.let { throw it }
        try {
            var offset = 0
            while (offset < data.size) {
                offset += appendChunk(data, offset)
                while (true) {
                    val payload = tryDecodeOne() ?: break
                    accept(payload, lastDecodedLength)
                }
            }
        } catch (cause: Exception) {
            reset()
            failure = cause
            throw cause
        }
    }

    private fun appendChunk(
        data: ByteArray,
        offset: Int,
    ): Int {
        val remainingCapacity =
            if (headersParsed) {
                checkNotNull(expectedContentLength) - byteBufferSize
            } else {
                JsonRpcCodec.MAX_HEADER_LENGTH - byteBufferSize
            }
        if (remainingCapacity <= 0) {
            throw JsonRpcProtocolException("Header exceeds ${JsonRpcCodec.MAX_HEADER_LENGTH} bytes")
        }
        val count = minOf(data.size - offset, remainingCapacity)
        ensureCapacity(byteBufferSize + count)
        data.copyInto(byteBuffer, byteBufferSize, offset, offset + count)
        byteBufferSize += count
        return count
    }

    /**
     * Reset the decoder state for a new stream.
     * After failure, the caller must discard the old stream; this does not resynchronize it.
     */
    public fun reset() {
        byteBuffer = ByteArray(0)
        byteBufferSize = 0
        expectedContentLength = null
        headersParsed = false
        headerSearchOffset = 0
        failure = null
    }

    private fun ensureCapacity(minCapacity: Int) {
        if (minCapacity <= byteBuffer.size) return
        val newCapacity =
            maxOf(minCapacity, byteBuffer.size * 2, DEFAULT_BUFFER_CAPACITY)
                .coerceAtMost(JsonRpcCodec.MAX_CONTENT_LENGTH)
        val newBuffer = ByteArray(newCapacity)
        byteBuffer.copyInto(newBuffer, 0, 0, byteBufferSize)
        byteBuffer = newBuffer
    }

    private companion object {
        private const val DEFAULT_BUFFER_CAPACITY = 1024
        private val headerDelimiterBytes = JsonRpcCodec.HEADER_DELIMITER.encodeToByteArray()
    }

    private fun consumeFromFront(count: Int) {
        byteBuffer.copyInto(byteBuffer, 0, count, byteBufferSize)
        byteBufferSize -= count
    }

    private fun drainPayloads(payloads: MutableList<Payload>) {
        while (true) {
            val payload = tryDecodeOne() ?: break
            payloads.add(payload)
        }
    }

    private fun tryDecodeOne(): Payload? {
        if (!headersParsed && !tryParseHeaders()) {
            return null
        }

        // Check if we have enough content (in bytes)
        val contentLength = expectedContentLength ?: return null
        if (byteBufferSize < contentLength) {
            return null // Need more data
        }

        // Extract and decode the message
        val jsonContentBytes = byteBuffer.copyOfRange(0, contentLength)
        limits?.let { JsonNestingGuard(it.maxJsonNesting, codec.allowsComments).check(jsonContentBytes) }
        val jsonContent = jsonContentBytes.decodeToString()
        consumeFromFront(contentLength)

        // Reset state for next message
        expectedContentLength = null
        headersParsed = false
        lastDecodedLength = contentLength

        return codec.decodePayload(jsonContent)
    }

    private fun tryParseHeaders(): Boolean {
        val delimiterBytes = headerDelimiterBytes
        val headerEndIndex = byteBuffer.indexOf(delimiterBytes, byteBufferSize, headerSearchOffset)
        if (headerEndIndex < 0) {
            if (byteBufferSize >= JsonRpcCodec.MAX_HEADER_LENGTH) {
                throw JsonRpcProtocolException("Header exceeds ${JsonRpcCodec.MAX_HEADER_LENGTH} bytes")
            }
            headerSearchOffset = maxOf(0, byteBufferSize - delimiterBytes.size + 1)
            return false
        }
        headerSearchOffset = 0

        val headerSection = byteBuffer.copyOfRange(0, headerEndIndex).decodeToString()
        expectedContentLength = parseHeaderLength(headerSection)
        validateContentTypeCharset(headerSection)
        limits?.let {
            if (checkNotNull(expectedContentLength) > it.maxFrameBodyBytes) {
                throw JsonRpcProtocolException("Frame body exceeds ${it.maxFrameBodyBytes} bytes")
            }
        }
        consumeFromFront(headerEndIndex + delimiterBytes.size)
        headersParsed = true
        return true
    }

    private fun parseHeaderLength(headerSection: String): Int {
        val lengthHeaders =
            headerSection.split(JsonRpcCodec.LINE_DELIMITER).filter {
                it.trim().startsWith(JsonRpcCodec.CONTENT_LENGTH_HEADER, ignoreCase = true)
            }
        if (lengthHeaders.size != 1) {
            val message = if (lengthHeaders.isEmpty()) "Missing Content-Length header" else "Duplicate Content-Length header"
            throw JsonRpcProtocolException(message)
        }
        return codec.parseContentLength(lengthHeaders.single())
            ?: throw JsonRpcProtocolException("Invalid Content-Length header")
    }

    /**
     * Rejects a Content-Type header that declares any charset other than UTF-8 (or legacy utf8).
     *
     * Parameters are split on semicolons without honouring quoted strings. That is sufficient for
     * LSP, whose Content-Type is a media type plus an optional charset; a quoted parameter value
     * containing a semicolon is not supported. Every Content-Type header and every charset
     * parameter present is validated, so the strictest one wins.
     */
    private fun validateContentTypeCharset(headerSection: String) {
        for (line in headerSection.split(JsonRpcCodec.LINE_DELIMITER)) {
            val trimmed = line.trim()
            if (!trimmed.startsWith("Content-Type:", ignoreCase = true)) continue
            val params = trimmed.substringAfter(':').split(';').drop(1)
            for (param in params) {
                val name = param.substringBefore('=', "").trim()
                if (!name.equals("charset", ignoreCase = true) || !param.contains('=')) continue
                val value =
                    param
                        .substringAfter('=')
                        .trim()
                        .removeSurrounding("\"")
                        .trim()
                if (!value.equals("utf-8", ignoreCase = true) && !value.equals("utf8", ignoreCase = true)) {
                    throw JsonRpcProtocolException("Unsupported Content-Type charset: $value")
                }
            }
        }
    }

    /**
     * Find the index of a byte array pattern within a byte array.
     * Only searches within the first [limit] bytes.
     * Returns -1 if not found.
     */
    private fun ByteArray.indexOf(
        pattern: ByteArray,
        limit: Int,
        start: Int,
    ): Int {
        if (pattern.isEmpty()) return 0
        if (pattern.size > limit) return -1

        outer@ for (i in start..(limit - pattern.size)) {
            for (j in pattern.indices) {
                if (this[i + j] != pattern[j]) continue@outer
            }
            return i
        }
        return -1
    }
}

/**
 * Exception thrown when JSON-RPC protocol is violated.
 */
public class JsonRpcProtocolException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)
