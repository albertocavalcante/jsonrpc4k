package io.jsonrpc4k.core

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import io.kotest.matchers.string.shouldStartWith
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertFailsWith

class CodecTest {
    private val codec = JsonRpcCodec.Default

    // ===== JsonRpcCodec.encode Tests =====

    @Test
    fun `encode message with Content-Length header`() {
        val message = NotificationMessage(method = "initialized")
        val encoded = codec.encode(message)
        val decoded = encoded.decodeToString()

        decoded shouldStartWith "Content-Length:"
        decoded shouldContain "\r\n\r\n"
        decoded shouldContain "\"method\":\"initialized\""
    }

    @Test
    fun `encode request message`() {
        val message =
            RequestMessage(
                id = RequestId.of(1),
                method = "test/method",
                params = buildJsonObject { put("key", JsonPrimitive("value")) },
            )
        val encoded = codec.encode(message)
        val decoded = encoded.decodeToString()

        decoded shouldContain "\"id\":1"
        decoded shouldContain "\"method\":\"test/method\""
        decoded shouldContain "\"params\":"
    }

    @Test
    fun `encode response message with result`() {
        val message =
            ResponseMessage.success(
                id = RequestId.of(42),
                result = JsonPrimitive("success"),
            )
        val encoded = codec.encode(message)
        val decoded = encoded.decodeToString()

        decoded shouldContain "\"id\":42"
        decoded shouldContain "\"result\":\"success\""
    }

    @Test
    fun `encode response message with error`() {
        val message =
            ResponseMessage.error(
                id = RequestId.of(1),
                error = ResponseError.methodNotFound("test/unknown"),
            )
        val encoded = codec.encode(message)
        val decoded = encoded.decodeToString()

        decoded shouldContain "\"error\":"
        decoded shouldContain "\"code\":-32601"
    }

    @Test
    fun `encode calculates correct Content-Length`() {
        val message = NotificationMessage(method = "test")
        val encoded = codec.encode(message)
        val decoded = encoded.decodeToString()

        // Extract the Content-Length value
        val headerEnd = decoded.indexOf("\r\n\r\n")
        val header = decoded.substring(0, headerEnd)
        val contentLengthValue = header.substringAfter("Content-Length:").trim().toInt()

        // The content after headers
        val content = decoded.substring(headerEnd + 4)

        // Content-Length should match the actual byte length
        contentLengthValue shouldBe content.encodeToByteArray().size
    }

    @Test
    fun `encode handles unicode content correctly`() {
        val message =
            NotificationMessage(
                method = "test",
                params = buildJsonObject { put("text", JsonPrimitive("Hello \u4e16\u754c")) },
            )
        val encoded = codec.encode(message)
        val decoded = encoded.decodeToString()

        // Extract Content-Length and verify it counts bytes, not characters
        val headerEnd = decoded.indexOf("\r\n\r\n")
        val header = decoded.substring(0, headerEnd)
        val contentLengthValue = header.substringAfter("Content-Length:").trim().toInt()
        val content = decoded.substring(headerEnd + 4)

        contentLengthValue shouldBe content.encodeToByteArray().size
    }

    // ===== JsonRpcCodec.encodeToJson Tests =====

    @Test
    fun `encodeToJson produces valid JSON without headers`() {
        val message = NotificationMessage(method = "test")
        val jsonStr = codec.encodeToJson(message)

        jsonStr shouldContain "\"jsonrpc\":\"2.0\""
        jsonStr shouldContain "\"method\":\"test\""
        jsonStr.shouldNot { it.contains("Content-Length") }
    }

    // ===== JsonRpcCodec.decodeFromJson Tests =====

    @Test
    fun `decodeFromJson decodes request message`() {
        val jsonStr = """{"jsonrpc":"2.0","id":1,"method":"test"}"""
        val message = codec.decodeFromJson(jsonStr)

        message.shouldBeInstanceOf<RequestMessage>()
        (message as RequestMessage).id shouldBe RequestId.of(1)
        message.method shouldBe "test"
    }

    @Test
    fun `decodeFromJson decodes notification message`() {
        val jsonStr = """{"jsonrpc":"2.0","method":"initialized"}"""
        val message = codec.decodeFromJson(jsonStr)

        message.shouldBeInstanceOf<NotificationMessage>()
        (message as NotificationMessage).method shouldBe "initialized"
    }

    @Test
    fun `decodeFromJson decodes response message`() {
        val jsonStr = """{"jsonrpc":"2.0","id":1,"result":"ok"}"""
        val message = codec.decodeFromJson(jsonStr)

        message.shouldBeInstanceOf<ResponseMessage>()
        (message as ResponseMessage).result shouldBe JsonPrimitive("ok")
    }

    // ===== JsonRpcCodec.parseContentLength Tests =====

    @Test
    fun `parseContentLength extracts correct value`() {
        codec.parseContentLength("Content-Length: 123") shouldBe 123
        codec.parseContentLength("content-length: 456") shouldBe 456
        codec.parseContentLength("Content-Type: application/json") shouldBe null
    }

    @Test
    fun `parseContentLength handles various formats`() {
        codec.parseContentLength("Content-Length:123") shouldBe 123
        codec.parseContentLength("Content-Length:  123  ") shouldBe 123
        codec.parseContentLength("CONTENT-LENGTH: 789") shouldBe 789
    }

    @Test
    fun `parseContentLength returns null for non-Content-Length headers`() {
        codec.parseContentLength("Content-Type: application/json") shouldBe null
        codec.parseContentLength("Accept: */*") shouldBe null
        codec.parseContentLength("") shouldBe null
        codec.parseContentLength("   ") shouldBe null
    }

    @Test
    fun `parseContentLength returns null for invalid numbers`() {
        codec.parseContentLength("Content-Length: abc") shouldBe null
        codec.parseContentLength("Content-Length: ") shouldBe null
        codec.parseContentLength("Content-Length: 12.5") shouldBe null
    }

    @Test
    fun `parseContentLength handles zero and large values`() {
        codec.parseContentLength("Content-Length: 0") shouldBe 0
        // Values within MAX_CONTENT_LENGTH (100 MB) should be accepted
        codec.parseContentLength("Content-Length: ${JsonRpcCodec.MAX_CONTENT_LENGTH}") shouldBe JsonRpcCodec.MAX_CONTENT_LENGTH
        // Values exceeding MAX_CONTENT_LENGTH should be rejected
        codec.parseContentLength("Content-Length: ${JsonRpcCodec.MAX_CONTENT_LENGTH + 1}") shouldBe null
        codec.parseContentLength("Content-Length: 999999999") shouldBe null
    }

    // ===== JsonRpcCodec constants Tests =====

    @Test
    fun `JsonRpcCodec constants are correct`() {
        JsonRpcCodec.CONTENT_LENGTH_HEADER shouldBe "Content-Length:"
        JsonRpcCodec.HEADER_DELIMITER shouldBe "\r\n\r\n"
        JsonRpcCodec.LINE_DELIMITER shouldBe "\r\n"
    }

    // ===== JsonRpcMessageDecoder Tests =====

    @Test
    fun `decode single message`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"initialized"}"""
        val contentBytes = content.encodeToByteArray()
        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        val messages = decoder.feed(input)
        messages shouldHaveSize 1
        messages[0].shouldBeInstanceOf<NotificationMessage>()
        (messages[0] as NotificationMessage).method shouldBe "initialized"
    }

    @Test
    fun `decode multiple messages in one chunk`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content1 = """{"jsonrpc":"2.0","method":"method1"}"""
        val content2 = """{"jsonrpc":"2.0","method":"method2"}"""
        val content1Bytes = content1.encodeToByteArray()
        val content2Bytes = content2.encodeToByteArray()
        val input =
            "Content-Length: ${content1Bytes.size}\r\n\r\n$content1" +
                "Content-Length: ${content2Bytes.size}\r\n\r\n$content2"

        val messages = decoder.feed(input)
        messages shouldHaveSize 2
        (messages[0] as NotificationMessage).method shouldBe "method1"
        (messages[1] as NotificationMessage).method shouldBe "method2"
    }

    @Test
    fun `decode message split across chunks`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        val fullMessage = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        // Split message in the middle
        val part1 = fullMessage.substring(0, 20)
        val part2 = fullMessage.substring(20)

        val messages1 = decoder.feed(part1)
        messages1 shouldHaveSize 0

        val messages2 = decoder.feed(part2)
        messages2 shouldHaveSize 1
        (messages2[0] as NotificationMessage).method shouldBe "test"
    }

    @Test
    fun `decode message split in header`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        val fullMessage = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        // Split in the middle of the header
        val part1 = "Content-"
        val part2 = fullMessage.substring(8)

        val messages1 = decoder.feed(part1)
        messages1.shouldBeEmpty()

        val messages2 = decoder.feed(part2)
        messages2 shouldHaveSize 1
    }

    @Test
    fun `decode message split at header delimiter`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        val header = "Content-Length: ${contentBytes.size}\r\n"

        // First chunk: header without delimiter
        decoder.feed(header).shouldBeEmpty()

        // Second chunk: delimiter
        decoder.feed("\r\n").shouldBeEmpty()

        // Third chunk: content
        val messages = decoder.feed(content)
        messages shouldHaveSize 1
    }

    @Test
    fun `decode message split in content`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        val header = "Content-Length: ${contentBytes.size}\r\n\r\n"

        decoder.feed(header).shouldBeEmpty()

        // Feed content byte by byte (first half)
        val half = content.length / 2
        decoder.feed(content.substring(0, half)).shouldBeEmpty()

        // Feed remaining content
        val messages = decoder.feed(content.substring(half))
        messages shouldHaveSize 1
    }

    @Test
    fun `decode request message`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","id":1,"method":"test/request","params":{"key":"value"}}"""
        val contentBytes = content.encodeToByteArray()
        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        val messages = decoder.feed(input)
        messages shouldHaveSize 1
        val request = messages[0] as RequestMessage
        request.id shouldBe RequestId.of(1)
        request.method shouldBe "test/request"
    }

    @Test
    fun `decode response message`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","id":1,"result":"success"}"""
        val contentBytes = content.encodeToByteArray()
        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        val messages = decoder.feed(input)
        messages shouldHaveSize 1
        val response = messages[0] as ResponseMessage
        response.id shouldBe RequestId.of(1)
        response.result shouldBe JsonPrimitive("success")
    }

    @Test
    fun `decode with multiple headers`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        val input = "Content-Length: ${contentBytes.size}\r\nContent-Type: application/json\r\n\r\n$content"

        val messages = decoder.feed(input)
        messages shouldHaveSize 1
        (messages[0] as NotificationMessage).method shouldBe "test"
    }

    @Test
    fun `decode throws on missing Content-Length header`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val input = "Content-Type: application/json\r\n\r\n{}"

        assertFailsWith<JsonRpcProtocolException> {
            decoder.feed(input)
        }
    }

    @Test
    fun `decode handles byte array input`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        val messages = decoder.feed(input.encodeToByteArray())
        messages shouldHaveSize 1
    }

    @Test
    fun `decoder reset clears state`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()

        // Feed partial message
        decoder.feed("Content-Length: ${contentBytes.size}\r\n")

        // Reset
        decoder.reset()

        // Now feed complete message
        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"
        val messages = decoder.feed(input)
        messages shouldHaveSize 1
    }

    @Test
    fun `decode three messages consecutively`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content1 = """{"jsonrpc":"2.0","method":"one"}"""
        val content2 = """{"jsonrpc":"2.0","method":"two"}"""
        val content3 = """{"jsonrpc":"2.0","method":"three"}"""
        val content1Bytes = content1.encodeToByteArray()
        val content2Bytes = content2.encodeToByteArray()
        val content3Bytes = content3.encodeToByteArray()

        val msg1 = "Content-Length: ${content1Bytes.size}\r\n\r\n$content1"
        val msg2 = "Content-Length: ${content2Bytes.size}\r\n\r\n$content2"
        val msg3 = "Content-Length: ${content3Bytes.size}\r\n\r\n$content3"

        // Feed all three at once
        val messages = decoder.feed(msg1 + msg2 + msg3)
        messages shouldHaveSize 3
        (messages[0] as NotificationMessage).method shouldBe "one"
        (messages[1] as NotificationMessage).method shouldBe "two"
        (messages[2] as NotificationMessage).method shouldBe "three"
    }

    @Test
    fun `decode handles empty buffer gracefully`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val messages = decoder.feed("")
        messages.shouldBeEmpty()
    }

    @Test
    fun `decode handles unicode in message`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test","params":{"text":"Hello \u4e16\u754c"}}"""
        val contentBytes = content.encodeToByteArray()
        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"

        val messages = decoder.feed(input)
        messages shouldHaveSize 1
    }

    // ===== BUG-001: UTF-8 byte length vs char length =====

    @Test
    fun `decode correctly handles multi-byte UTF-8 characters`() {
        val decoder = JsonRpcMessageDecoder(codec)
        // Chinese characters are 3 bytes each in UTF-8
        // "Hello 世界" = 6 ASCII + 1 space + 6 bytes (2 Chinese chars) = 13 bytes, but 9 chars
        val content = """{"jsonrpc":"2.0","method":"test","params":{"text":"Hello 世界"}}"""
        val contentBytes = content.encodeToByteArray()

        // Verify byte length differs from char length for this content
        check(contentBytes.size != content.length) { "Test precondition: byte length should differ from char length" }

        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"
        val messages = decoder.feed(input)
        messages shouldHaveSize 1
    }

    @Test
    fun `decode correctly handles emoji characters`() {
        val decoder = JsonRpcMessageDecoder(codec)
        // Emoji can be 4 bytes in UTF-8
        val content = """{"jsonrpc":"2.0","method":"test","params":{"emoji":"\uD83D\uDE00"}}"""
        val contentBytes = content.encodeToByteArray()

        val input = "Content-Length: ${contentBytes.size}\r\n\r\n$content"
        val messages = decoder.feed(input)
        messages shouldHaveSize 1
    }

    @Test
    fun `decode multiple messages with unicode correctly`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content1 = """{"jsonrpc":"2.0","method":"test1","params":{"text":"日本語"}}"""
        val content2 = """{"jsonrpc":"2.0","method":"test2","params":{"text":"한국어"}}"""
        val content1Bytes = content1.encodeToByteArray()
        val content2Bytes = content2.encodeToByteArray()

        val input =
            "Content-Length: ${content1Bytes.size}\r\n\r\n$content1" +
                "Content-Length: ${content2Bytes.size}\r\n\r\n$content2"

        val messages = decoder.feed(input)
        messages shouldHaveSize 2
    }

    // ===== BUG-005: Negative Content-Length =====
    // Note: The digit-only validation (SEC-003) now rejects negative values by returning null,
    // since '-' is not a digit. This is equally effective at preventing negative Content-Length.

    @Test
    fun `parseContentLength returns null for negative value`() {
        // Negative values are rejected because '-' is not a digit
        codec.parseContentLength("Content-Length: -1") shouldBe null
    }

    @Test
    fun `parseContentLength returns null for large negative value`() {
        // Negative values are rejected because '-' is not a digit
        codec.parseContentLength("Content-Length: -999999") shouldBe null
    }

    // ===== BUG-008: Duplicate Content-Length headers =====

    @Test
    fun `decode throws on duplicate Content-Length headers`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val contentBytes = content.encodeToByteArray()
        // Two Content-Length headers
        val input = "Content-Length: ${contentBytes.size}\r\nContent-Length: ${contentBytes.size}\r\n\r\n$content"

        assertFailsWith<JsonRpcProtocolException> {
            decoder.feed(input)
        }.message shouldContain "Duplicate"
    }

    @Test
    fun `decode throws on conflicting Content-Length headers`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        // Two different Content-Length values
        val input = "Content-Length: 10\r\nContent-Length: 20\r\n\r\n$content"

        assertFailsWith<JsonRpcProtocolException> {
            decoder.feed(input)
        }.message shouldContain "Duplicate"
    }

    // ===== JsonRpcProtocolException Tests =====

    @Test
    fun `JsonRpcProtocolException has correct message`() {
        val exception = JsonRpcProtocolException("Test error")
        exception.message shouldBe "Test error"
    }

    @Test
    fun `JsonRpcProtocolException can have cause`() {
        val cause = RuntimeException("Cause")
        val exception = JsonRpcProtocolException("Test error", cause)
        exception.message shouldBe "Test error"
        exception.cause shouldBe cause
    }

    // ===== Default instance Tests =====

    @Test
    fun `JsonRpcCodec Default instance is available`() {
        val defaultCodec = JsonRpcCodec.Default
        defaultCodec.shouldBeInstanceOf<JsonRpcCodec>()
    }

    @Test
    fun `JsonRpcCodec defaultJson ignores unknown keys`() {
        val json = JsonRpcCodec.defaultJson
        // This should not throw even with unknown key
        val message =
            json.decodeFromString(
                Message.serializer(),
                """{"jsonrpc":"2.0","method":"test","unknownField":"value"}""",
            )
        message.shouldBeInstanceOf<NotificationMessage>()
    }

    // Helper extension for negative assertion
    private fun <T> T.shouldNot(predicate: (T) -> Boolean) {
        if (predicate(this)) {
            throw AssertionError("Value should not match predicate: $this")
        }
    }
}
