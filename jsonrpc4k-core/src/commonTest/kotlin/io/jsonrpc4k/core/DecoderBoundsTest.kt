package io.jsonrpc4k.core

import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertSame
import kotlin.test.assertTrue

class DecoderBoundsTest {
    private val codec = JsonRpcCodec.Default
    private val headerLimit = 16 * 1024
    private val content = """{"jsonrpc":"2.0","method":"bounded"}"""

    @Test
    fun `unterminated headers fail at the header limit`() {
        val decoder = JsonRpcMessageDecoder(codec)
        assertTrue(decoder.feed("X".repeat(headerLimit - 1)).isEmpty())
        assertFailsWith<JsonRpcProtocolException> { decoder.feed("X") }
    }

    @Test
    fun `large unterminated single feed is rejected`() {
        assertFailsWith<JsonRpcProtocolException> {
            JsonRpcMessageDecoder(codec).feed(ByteArray(headerLimit * 64) { 'X'.code.toByte() })
        }
    }

    @Test
    fun `header at the limit accepts a fragmented delimiter`() {
        val header = headerOfSize(headerLimit)
        val decoder = JsonRpcMessageDecoder(codec)
        assertTrue(decoder.feed(header.dropLast(1)).isEmpty())
        val messages = decoder.feed(header.takeLast(1) + content).messages()
        assertTrue(messages == listOf(NotificationMessage(method = "bounded")))
    }

    @Test
    fun `terminated header beyond the limit is rejected`() {
        assertFailsWith<JsonRpcProtocolException> {
            JsonRpcMessageDecoder(codec).feed(headerOfSize(headerLimit + 1) + content)
        }
    }

    @Test
    fun `invalid duplicate length is rejected in either order`() {
        val valid = "Content-Length: ${content.encodeToByteArray().size}"
        for (headers in listOf("Content-Length: -1\r\n$valid", "$valid\r\nContent-Length: -1")) {
            assertFailsWith<JsonRpcProtocolException> {
                JsonRpcMessageDecoder(codec).feed("$headers\r\n\r\n$content")
            }
        }
    }

    @Test
    fun `decoder failure is terminal until explicit reset`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val failure = assertFailsWith<JsonRpcProtocolException> { decoder.feed("Content-Length: x\r\n\r\n") }
        val repeated = assertFailsWith<JsonRpcProtocolException> { decoder.feed(headerOfSize(64) + content) }
        assertSame(failure, repeated)
        decoder.reset()
        assertTrue(decoder.feed(headerOfSize(64) + content).messages() == listOf(NotificationMessage(method = "bounded")))
    }

    @Test
    fun `malformed body decodes to a payload and the stream continues`() {
        val decoder = JsonRpcMessageDecoder(codec)
        assertTrue(decoder.feed("Content-Length: 1\r\n\r\n{") == listOf(Payload.Invalid.PARSE_ERROR))
        assertTrue(decoder.feed(headerOfSize(64) + content).messages() == listOf(NotificationMessage(method = "bounded")))
    }

    @Test
    fun `large valid bodies and coalesced frames are independent of chunk size`() {
        val expected =
            (1..3).map { index ->
                NotificationMessage(method = "message-$index", params = JsonPrimitive("世界🌍".repeat(4_000)))
            }
        val bytes = expected.fold(ByteArray(0)) { data, message -> data + codec.encode(message) }
        assertTrue(bytes.size > headerLimit * 4)
        for (chunkSize in listOf(1, 3, 29, 4096, Int.MAX_VALUE)) {
            val decoder = JsonRpcMessageDecoder(codec)
            val actual = mutableListOf<Message>()
            var offset = 0
            while (offset < bytes.size) {
                val count = minOf(chunkSize, bytes.size - offset)
                actual.addAll(decoder.feed(bytes.copyOfRange(offset, offset + count)).messages())
                offset += count
            }
            assertTrue(actual == expected, "Chunk size $chunkSize altered valid UTF-8 messages")
        }
    }

    @Test
    fun `existing body length ceiling is retained`() {
        val decoder = JsonRpcMessageDecoder(codec)
        assertTrue(decoder.feed("Content-Length: ${JsonRpcCodec.MAX_CONTENT_LENGTH}\r\n\r\n").isEmpty())
        assertFailsWith<JsonRpcProtocolException> {
            JsonRpcMessageDecoder(codec).feed("Content-Length: ${JsonRpcCodec.MAX_CONTENT_LENGTH + 1}\r\n\r\n")
        }
    }

    private fun headerOfSize(size: Int): String {
        val prefix = "Content-Length: ${content.encodeToByteArray().size}\r\nX-Extra: "
        val suffix = "\r\n\r\n"
        return prefix + "x".repeat(size - prefix.length - suffix.length) + suffix
    }
}
