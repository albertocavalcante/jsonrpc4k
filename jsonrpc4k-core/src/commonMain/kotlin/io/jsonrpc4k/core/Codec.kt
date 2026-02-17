package io.jsonrpc4k.core

import kotlinx.serialization.json.Json

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
    /**
     * Encode a message to wire format with Content-Length header.
     */
    public fun encode(message: Message): ByteArray {
        val content = json.encodeToString(Message.serializer(), message)
        val contentBytes = content.encodeToByteArray()
        val header = "Content-Length: ${contentBytes.size}\r\n\r\n"
        return header.encodeToByteArray() + contentBytes
    }

    /**
     * Encode a message to JSON string (without headers).
     */
    public fun encodeToJson(message: Message): String = json.encodeToString(Message.serializer(), message)

    /**
     * Decode a JSON string to a Message.
     */
    public fun decodeFromJson(jsonString: String): Message = json.decodeFromString(Message.serializer(), jsonString)

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
 * A streaming decoder for JSON-RPC messages.
 * Handles partial reads and buffering.
 *
 * Note: Content-Length specifies the number of bytes, not characters.
 * This decoder maintains a byte buffer to correctly handle multi-byte UTF-8 characters.
 */
public class JsonRpcMessageDecoder(
    private val codec: JsonRpcCodec = JsonRpcCodec.Default,
) {
    private var byteBuffer = ByteArray(0)
    private var byteBufferSize = 0
    private var expectedContentLength: Int? = null
    private var headersParsed = false

    /**
     * Feed data into the decoder.
     * Returns a list of complete messages that were decoded.
     */
    public fun feed(data: String): List<Message> = feed(data.encodeToByteArray())

    /**
     * Feed data into the decoder.
     * Returns a list of complete messages that were decoded.
     */
    public fun feed(data: ByteArray): List<Message> {
        ensureCapacity(byteBufferSize + data.size)
        data.copyInto(byteBuffer, byteBufferSize)
        byteBufferSize += data.size
        return drainMessages()
    }

    /**
     * Reset the decoder state.
     */
    public fun reset() {
        byteBuffer = ByteArray(0)
        byteBufferSize = 0
        expectedContentLength = null
        headersParsed = false
    }

    private fun ensureCapacity(minCapacity: Int) {
        if (minCapacity <= byteBuffer.size) return
        val newCapacity = maxOf(minCapacity, byteBuffer.size * 2, DEFAULT_BUFFER_CAPACITY)
        val newBuffer = ByteArray(newCapacity)
        byteBuffer.copyInto(newBuffer, 0, 0, byteBufferSize)
        byteBuffer = newBuffer
    }

    private companion object {
        private const val DEFAULT_BUFFER_CAPACITY = 1024
    }

    private fun consumeFromFront(count: Int) {
        byteBuffer.copyInto(byteBuffer, 0, count, byteBufferSize)
        byteBufferSize -= count
    }

    private fun drainMessages(): List<Message> {
        val messages = mutableListOf<Message>()

        while (true) {
            val message = tryDecodeOne() ?: break
            messages.add(message)
        }

        return messages
    }

    private fun tryDecodeOne(): Message? {
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
        val jsonContent = jsonContentBytes.decodeToString()
        consumeFromFront(contentLength)

        // Reset state for next message
        expectedContentLength = null
        headersParsed = false

        return codec.decodeFromJson(jsonContent)
    }

    private fun tryParseHeaders(): Boolean {
        val delimiterBytes = JsonRpcCodec.HEADER_DELIMITER.encodeToByteArray()
        val headerEndIndex = byteBuffer.indexOf(delimiterBytes, byteBufferSize)
        if (headerEndIndex < 0) return false

        val headerSection = byteBuffer.copyOfRange(0, headerEndIndex).decodeToString()
        val lines = headerSection.split(JsonRpcCodec.LINE_DELIMITER)

        var foundContentLength = false
        for (line in lines) {
            val contentLength = codec.parseContentLength(line)
            if (contentLength != null) {
                if (foundContentLength) {
                    throw JsonRpcProtocolException("Duplicate Content-Length header")
                }
                expectedContentLength = contentLength
                foundContentLength = true
            }
        }

        if (expectedContentLength == null) {
            throw JsonRpcProtocolException("Missing Content-Length header")
        }

        consumeFromFront(headerEndIndex + delimiterBytes.size)
        headersParsed = true
        return true
    }

    /**
     * Find the index of a byte array pattern within a byte array.
     * Only searches within the first [limit] bytes.
     * Returns -1 if not found.
     */
    private fun ByteArray.indexOf(
        pattern: ByteArray,
        limit: Int,
    ): Int {
        if (pattern.isEmpty()) return 0
        if (pattern.size > limit) return -1

        outer@ for (i in 0..(limit - pattern.size)) {
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
