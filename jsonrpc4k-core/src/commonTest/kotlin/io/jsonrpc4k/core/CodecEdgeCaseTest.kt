package io.jsonrpc4k.core

import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertFailsWith

/**
 * Edge case tests for JsonRpcCodec and JsonRpcMessageDecoder.
 * Tests parsing edge cases, error conditions, and unicode handling.
 */
class CodecEdgeCaseTest {
    private val codec = JsonRpcCodec.Default

    // ==================== Content-Length Parsing Tests ====================

    @Test
    fun `decoder handles zero Content-Length`() {
        val decoder = JsonRpcMessageDecoder(codec)
        // Zero content length should result in parsing error (empty JSON)
        val message = "Content-Length: 0\r\n\r\n"
        assertFailsWith<Exception> {
            decoder.feed(message)
        }
    }

    @Test
    fun `decoder handles very large Content-Length header value`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        // The Content-Length must match actual content, but header can specify large value
        val message = "Content-Length: ${content.length}\r\n\r\n$content"
        val parsed = decoder.feed(message)
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles Content-Length with leading zeros`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        // Leading zeros in Content-Length value
        val message = "Content-Length: 00${content.length}\r\n\r\n$content"
        val parsed = decoder.feed(message)
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles Content-Type header - ignored`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        // Content-Type header should be ignored
        val message = "Content-Length: ${content.length}\r\nContent-Type: application/vscode-jsonrpc; charset=utf-8\r\n\r\n$content"
        val parsed = decoder.feed(message)
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles headers in different order`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        // Content-Type before Content-Length
        val message = "Content-Type: application/json\r\nContent-Length: ${content.length}\r\n\r\n$content"
        val parsed = decoder.feed(message)
        parsed.size shouldBe 1
    }

    // ==================== Unicode Handling Tests ====================

    @Test
    fun `codec handles unicode in method name`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test/方法",
            )
        val encoded = codec.encode(request)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
        (parsed.first() as RequestMessage).method shouldBe "test/方法"
    }

    @Test
    fun `codec handles unicode in params`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test",
                params =
                    buildJsonObject {
                        put("text", "Hello 世界 🌍")
                    },
            )
        val encoded = codec.encode(request)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
        val decoded = parsed.first() as RequestMessage
        decoded.params.toString() shouldContain "世界"
        decoded.params.toString() shouldContain "🌍"
    }

    @Test
    fun `codec handles emoji in params`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test",
                params =
                    buildJsonObject {
                        put("emoji", "👍👎🎉✨🔥")
                    },
            )
        val encoded = codec.encode(request)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
        val decoded = parsed.first() as RequestMessage
        decoded.params.toString() shouldContain "👍"
    }

    @Test
    fun `codec handles special characters in string`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test",
                params =
                    buildJsonObject {
                        put("text", "line1\nline2\ttabbed\r\nwindows")
                    },
            )
        val encoded = codec.encode(request)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
    }

    @Test
    fun `codec handles backslash and quotes in string`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test",
                params =
                    buildJsonObject {
                        put("path", "C:\\Users\\test\\file.txt")
                        put("quoted", "He said \"hello\"")
                    },
            )
        val encoded = codec.encode(request)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
    }

    // ==================== Streaming/Chunked Input Tests ====================

    @Test
    fun `decoder handles message split at header-body boundary`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""

        // Split at the header-body boundary
        decoder.feed("Content-Length: ${content.length}\r\n\r")
        val parsed = decoder.feed("\n$content")
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles message split within header`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""

        // Split within the header
        decoder.feed("Content-Len")
        decoder.feed("gth: ${content.length}")
        decoder.feed("\r\n\r\n")
        val parsed = decoder.feed(content)
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles message split within content`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""

        decoder.feed("Content-Length: ${content.length}\r\n\r\n")
        decoder.feed(content.substring(0, 10))
        val parsed = decoder.feed(content.substring(10))
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles single byte at a time`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"x"}"""
        val message = "Content-Length: ${content.length}\r\n\r\n$content"

        var parsed = emptyList<Message>()
        for (char in message) {
            parsed = decoder.feed(char.toString())
        }
        parsed.size shouldBe 1
    }

    @Test
    fun `decoder handles multiple messages in single feed`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content1 = """{"jsonrpc":"2.0","method":"test1"}"""
        val content2 = """{"jsonrpc":"2.0","method":"test2"}"""
        val content3 = """{"jsonrpc":"2.0","method":"test3"}"""

        val combined =
            "Content-Length: ${content1.length}\r\n\r\n$content1" +
                "Content-Length: ${content2.length}\r\n\r\n$content2" +
                "Content-Length: ${content3.length}\r\n\r\n$content3"

        val parsed = decoder.feed(combined)
        parsed.size shouldBe 3
    }

    // ==================== Reset Functionality Tests ====================

    @Test
    fun `decoder reset clears buffer`() {
        val decoder = JsonRpcMessageDecoder(codec)

        // Feed partial message
        decoder.feed("Content-Length: 100\r\n\r\n{")

        // Reset
        decoder.reset()

        // Feed new complete message
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val message = "Content-Length: ${content.length}\r\n\r\n$content"
        val parsed = decoder.feed(message)
        parsed.size shouldBe 1
    }

    // ==================== RequestId Tests ====================

    @Test
    fun `RequestId number serialization`() {
        val id = RequestId.of(12345L)
        (id is RequestId.NumberId) shouldBe true
        (id as RequestId.NumberId).value shouldBe 12345L
    }

    @Test
    fun `RequestId string serialization`() {
        val id = RequestId.of("req-123")
        (id is RequestId.StringId) shouldBe true
        (id as RequestId.StringId).value shouldBe "req-123"
    }

    @Test
    fun `RequestId zero`() {
        val id = RequestId.of(0L)
        (id as RequestId.NumberId).value shouldBe 0L
    }

    @Test
    fun `RequestId negative`() {
        val id = RequestId.of(-1L)
        (id as RequestId.NumberId).value shouldBe -1L
    }

    @Test
    fun `RequestId max long`() {
        val id = RequestId.of(Long.MAX_VALUE)
        (id as RequestId.NumberId).value shouldBe Long.MAX_VALUE
    }

    @Test
    fun `RequestId empty string`() {
        val id = RequestId.of("")
        (id as RequestId.StringId).value shouldBe ""
    }

    // ==================== Response Message Tests ====================

    @Test
    fun `ResponseMessage success with JsonPrimitive result`() {
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result = JsonPrimitive("hello"),
            )
        response.result shouldBe JsonPrimitive("hello")
        response.error shouldBe null
    }

    @Test
    fun `ResponseMessage success with numeric result`() {
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result = JsonPrimitive(42),
            )
        response.result shouldBe JsonPrimitive(42)
    }

    @Test
    fun `ResponseMessage success with boolean result`() {
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result = JsonPrimitive(true),
            )
        response.result shouldBe JsonPrimitive(true)
    }

    @Test
    fun `ResponseMessage success with object result`() {
        val result =
            buildJsonObject {
                put("key", "value")
                put("number", 42)
            }
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result = result,
            )
        response.result?.toString() shouldContain "key"
    }

    @Test
    fun `ResponseMessage error creation`() {
        val response =
            ResponseMessage.error(
                id = RequestId.of(1),
                error = ResponseError.methodNotFound("test/unknown"),
            )
        response.error?.code shouldBe ResponseError.METHOD_NOT_FOUND
        response.error?.message shouldBe "Method not found: test/unknown"
        response.result shouldBe null
    }

    @Test
    fun `ResponseMessage error with data`() {
        val response =
            ResponseMessage.error(
                id = RequestId.of(1),
                error = ResponseError.invalidParams("Invalid params", JsonPrimitive("extra info")),
            )
        response.error?.data shouldBe JsonPrimitive("extra info")
    }

    // ==================== Error Code Tests ====================

    @Test
    fun `ResponseError code values are correct`() {
        ResponseError.PARSE_ERROR shouldBe -32700
        ResponseError.INVALID_REQUEST shouldBe -32600
        ResponseError.METHOD_NOT_FOUND shouldBe -32601
        ResponseError.INVALID_PARAMS shouldBe -32602
        ResponseError.INTERNAL_ERROR shouldBe -32603
        ResponseError.SERVER_NOT_INITIALIZED shouldBe -32002
        ResponseError.UNKNOWN_ERROR_CODE shouldBe -32001
        ResponseError.REQUEST_CANCELLED shouldBe -32800
        ResponseError.CONTENT_MODIFIED shouldBe -32801
    }

    // ==================== Large Content Tests ====================

    @Test
    fun `codec handles large request`() {
        val largeText = "x".repeat(100_000)
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test",
                params =
                    buildJsonObject {
                        put("content", largeText)
                    },
            )
        val encoded = codec.encode(request)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
    }

    @Test
    fun `codec handles large response`() {
        val largeText = "y".repeat(100_000)
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result = JsonPrimitive(largeText),
            )
        val encoded = codec.encode(response)
        val decoder = JsonRpcMessageDecoder(codec)
        val parsed = decoder.feed(encoded)
        parsed.size shouldBe 1
    }
}
