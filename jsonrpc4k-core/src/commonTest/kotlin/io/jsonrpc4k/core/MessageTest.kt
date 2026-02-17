package io.jsonrpc4k.core

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonArray
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertFailsWith

class MessageTest {
    private val json =
        Json {
            ignoreUnknownKeys = true
            encodeDefaults = false
        }

    // ===== RequestId Tests =====

    @Test
    fun `RequestId with string value`() {
        val id = RequestId.of("uuid-123")
        id.shouldBeInstanceOf<RequestId.StringId>()
        id.toString() shouldBe "uuid-123"
    }

    @Test
    fun `RequestId with number value`() {
        val id = RequestId.of(42)
        id.shouldBeInstanceOf<RequestId.NumberId>()
        id.toString() shouldBe "42"
    }

    @Test
    fun `RequestId with zero value`() {
        val id = RequestId.of(0)
        id.shouldBeInstanceOf<RequestId.NumberId>()
        id.toString() shouldBe "0"
    }

    @Test
    fun `RequestId with negative number value`() {
        val id = RequestId.of(-1)
        id.shouldBeInstanceOf<RequestId.NumberId>()
        id.toString() shouldBe "-1"
    }

    @Test
    fun `RequestId with large number value`() {
        val id = RequestId.of(Long.MAX_VALUE)
        id.shouldBeInstanceOf<RequestId.NumberId>()
        id.toString() shouldBe Long.MAX_VALUE.toString()
    }

    @Test
    fun `RequestId with empty string value`() {
        val id = RequestId.of("")
        id.shouldBeInstanceOf<RequestId.StringId>()
        id.toString() shouldBe ""
    }

    @Test
    fun `RequestId equality for NumberId`() {
        val id1 = RequestId.of(42)
        val id2 = RequestId.of(42)
        val id3 = RequestId.of(43)

        id1 shouldBe id2
        id1 shouldNotBe id3
    }

    @Test
    fun `RequestId equality for StringId`() {
        val id1 = RequestId.of("test")
        val id2 = RequestId.of("test")
        val id3 = RequestId.of("other")

        id1 shouldBe id2
        id1 shouldNotBe id3
    }

    @Test
    fun `RequestId NumberId and StringId are not equal`() {
        val numberId = RequestId.of(42)
        val stringId = RequestId.of("42")

        numberId shouldNotBe stringId
    }

    @Test
    fun `RequestId serialization for NumberId`() {
        val id = RequestId.of(123)
        val encoded = json.encodeToString(RequestIdSerializer, id)
        encoded shouldBe "123"

        val decoded = json.decodeFromString(RequestIdSerializer, encoded)
        decoded shouldBe id
    }

    @Test
    fun `RequestId serialization for StringId`() {
        val id = RequestId.of("test-id")
        val encoded = json.encodeToString(RequestIdSerializer, id)
        encoded shouldBe "\"test-id\""

        val decoded = json.decodeFromString(RequestIdSerializer, encoded)
        decoded shouldBe id
    }

    // ===== RequestMessage Tests =====

    @Test
    fun `RequestMessage serialization roundtrip`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "textDocument/completion",
                params =
                    buildJsonObject {
                        put(
                            "textDocument",
                            buildJsonObject {
                                put("uri", JsonPrimitive("file:///test.kt"))
                            },
                        )
                    },
            )

        val encoded = json.encodeToString(Message.serializer(), request)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedRequest = decoded.shouldBeInstanceOf<RequestMessage>()
        decodedRequest.id shouldBe RequestId.of(1)
        decodedRequest.method shouldBe "textDocument/completion"
    }

    @Test
    fun `RequestMessage with string id`() {
        val request =
            RequestMessage(
                id = RequestId.of("unique-id-123"),
                method = "test/method",
            )

        val encoded = json.encodeToString(Message.serializer(), request)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedRequest = decoded.shouldBeInstanceOf<RequestMessage>()
        decodedRequest.id shouldBe RequestId.of("unique-id-123")
    }

    @Test
    fun `RequestMessage with null params`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test/method",
                params = null,
            )

        val encoded = json.encodeToString(Message.serializer(), request)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedRequest = decoded.shouldBeInstanceOf<RequestMessage>()
        decodedRequest.params shouldBe null
    }

    @Test
    fun `RequestMessage with array params`() {
        val request =
            RequestMessage(
                id = RequestId.of(1),
                method = "test/method",
                params =
                    buildJsonArray {
                        add(JsonPrimitive(1))
                        add(JsonPrimitive("two"))
                        add(JsonPrimitive(true))
                    },
            )

        val encoded = json.encodeToString(Message.serializer(), request)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedRequest = decoded.shouldBeInstanceOf<RequestMessage>()
        decodedRequest.params shouldNotBe null
    }

    @Test
    fun `RequestMessage has correct jsonrpc version`() {
        val request = RequestMessage(id = RequestId.of(1), method = "test")
        request.jsonrpc shouldBe JSONRPC_VERSION
        request.jsonrpc shouldBe "2.0"
    }

    // ===== NotificationMessage Tests =====

    @Test
    fun `NotificationMessage serialization roundtrip`() {
        val notification =
            NotificationMessage(
                method = "initialized",
                params = null,
            )

        val encoded = json.encodeToString(Message.serializer(), notification)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedNotification = decoded.shouldBeInstanceOf<NotificationMessage>()
        decodedNotification.method shouldBe "initialized"
    }

    @Test
    fun `NotificationMessage with params`() {
        val notification =
            NotificationMessage(
                method = "textDocument/didOpen",
                params =
                    buildJsonObject {
                        put(
                            "textDocument",
                            buildJsonObject {
                                put("uri", JsonPrimitive("file:///test.kt"))
                                put("languageId", JsonPrimitive("kotlin"))
                                put("version", JsonPrimitive(1))
                                put("text", JsonPrimitive("fun main() {}"))
                            },
                        )
                    },
            )

        val encoded = json.encodeToString(Message.serializer(), notification)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedNotification = decoded.shouldBeInstanceOf<NotificationMessage>()
        decodedNotification.method shouldBe "textDocument/didOpen"
        decodedNotification.params shouldNotBe null
    }

    @Test
    fun `NotificationMessage has correct jsonrpc version`() {
        val notification = NotificationMessage(method = "test")
        notification.jsonrpc shouldBe JSONRPC_VERSION
    }

    // ===== ResponseMessage Tests =====

    @Test
    fun `ResponseMessage with result`() {
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result = JsonPrimitive("success"),
            )

        val encoded = json.encodeToString(Message.serializer(), response)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedResponse = decoded.shouldBeInstanceOf<ResponseMessage>()
        decodedResponse.id shouldBe RequestId.of(1)
        decodedResponse.result shouldBe JsonPrimitive("success")
        decodedResponse.error shouldBe null
    }

    @Test
    fun `ResponseMessage with error`() {
        val response =
            ResponseMessage.error(
                id = RequestId.of(1),
                error = ResponseError.methodNotFound("unknown/method"),
            )

        val encoded = json.encodeToString(Message.serializer(), response)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedResponse = decoded.shouldBeInstanceOf<ResponseMessage>()
        decodedResponse.error?.code shouldBe ResponseError.METHOD_NOT_FOUND
    }

    @Test
    fun `ResponseMessage with null result`() {
        val response = ResponseMessage.success(id = RequestId.of(1), result = null)

        val encoded = json.encodeToString(Message.serializer(), response)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedResponse = decoded.shouldBeInstanceOf<ResponseMessage>()
        decodedResponse.result shouldBe null
        decodedResponse.error shouldBe null
    }

    @Test
    fun `ResponseMessage with JsonNull result`() {
        val response = ResponseMessage.success(id = RequestId.of(1), result = JsonNull)

        val encoded = json.encodeToString(Message.serializer(), response)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedResponse = decoded.shouldBeInstanceOf<ResponseMessage>()
        // Note: JsonNull gets serialized as "null" in JSON, and when deserialized
        // back to JsonElement?, kotlinx.serialization converts it to Kotlin null.
        // This is expected behavior - both represent "null result" semantically.
        decodedResponse.result shouldBe null
        decodedResponse.error shouldBe null
    }

    @Test
    fun `ResponseMessage with complex result object`() {
        val response =
            ResponseMessage.success(
                id = RequestId.of(1),
                result =
                    buildJsonObject {
                        put("capabilities", buildJsonObject { })
                        put(
                            "serverInfo",
                            buildJsonObject {
                                put("name", JsonPrimitive("test-server"))
                                put("version", JsonPrimitive("1.0.0"))
                            },
                        )
                    },
            )

        val encoded = json.encodeToString(Message.serializer(), response)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedResponse = decoded.shouldBeInstanceOf<ResponseMessage>()
        decodedResponse.result shouldNotBe null
    }

    @Test
    fun `ResponseMessage error with null id`() {
        val response =
            ResponseMessage.error(
                id = null,
                error = ResponseError.parseError("Parse error"),
            )

        val encoded = json.encodeToString(Message.serializer(), response)
        val decoded = json.decodeFromString(Message.serializer(), encoded)

        val decodedResponse = decoded.shouldBeInstanceOf<ResponseMessage>()
        decodedResponse.id shouldBe null
        decodedResponse.error?.code shouldBe ResponseError.PARSE_ERROR
    }

    @Test
    fun `ResponseMessage has correct jsonrpc version`() {
        val response = ResponseMessage.success(id = RequestId.of(1), result = null)
        response.jsonrpc shouldBe JSONRPC_VERSION
    }

    // ===== ResponseError Tests =====

    @Test
    fun `ResponseError parseError`() {
        val error = ResponseError.parseError("Invalid JSON")
        error.code shouldBe ResponseError.PARSE_ERROR
        error.message shouldBe "Invalid JSON"
        error.data shouldBe null
    }

    @Test
    fun `ResponseError parseError with data`() {
        val errorData = JsonPrimitive("additional info")
        val error = ResponseError.parseError("Invalid JSON", errorData)
        error.code shouldBe ResponseError.PARSE_ERROR
        error.data shouldBe errorData
    }

    @Test
    fun `ResponseError invalidRequest`() {
        val error = ResponseError.invalidRequest("Missing method field")
        error.code shouldBe ResponseError.INVALID_REQUEST
        error.message shouldBe "Missing method field"
    }

    @Test
    fun `ResponseError methodNotFound`() {
        val error = ResponseError.methodNotFound("unknown/method")
        error.code shouldBe ResponseError.METHOD_NOT_FOUND
        error.message shouldBe "Method not found: unknown/method"
    }

    @Test
    fun `ResponseError invalidParams`() {
        val error = ResponseError.invalidParams("Expected object, got array")
        error.code shouldBe ResponseError.INVALID_PARAMS
        error.message shouldBe "Expected object, got array"
    }

    @Test
    fun `ResponseError internalError`() {
        val error = ResponseError.internalError("Unexpected null pointer")
        error.code shouldBe ResponseError.INTERNAL_ERROR
        error.message shouldBe "Unexpected null pointer"
    }

    @Test
    fun `ResponseError serverNotInitialized`() {
        val error = ResponseError.serverNotInitialized()
        error.code shouldBe ResponseError.SERVER_NOT_INITIALIZED
        error.message shouldBe "Server not initialized"
    }

    @Test
    fun `ResponseError requestCancelled`() {
        val error = ResponseError.requestCancelled()
        error.code shouldBe ResponseError.REQUEST_CANCELLED
        error.message shouldBe "Request cancelled"
    }

    @Test
    fun `ResponseError contentModified`() {
        val error = ResponseError.contentModified()
        error.code shouldBe ResponseError.CONTENT_MODIFIED
        error.message shouldBe "Content modified"
    }

    @Test
    fun `ResponseError error codes are correct`() {
        ResponseError.PARSE_ERROR shouldBe -32700
        ResponseError.INVALID_REQUEST shouldBe -32600
        ResponseError.METHOD_NOT_FOUND shouldBe -32601
        ResponseError.INVALID_PARAMS shouldBe -32602
        ResponseError.INTERNAL_ERROR shouldBe -32603
        ResponseError.SERVER_NOT_INITIALIZED shouldBe -32002
        ResponseError.UNKNOWN_ERROR_CODE shouldBe -32001
        ResponseError.REQUEST_FAILED shouldBe -32803
        ResponseError.SERVER_CANCELLED shouldBe -32802
        ResponseError.CONTENT_MODIFIED shouldBe -32801
        ResponseError.REQUEST_CANCELLED shouldBe -32800
    }

    @Test
    fun `ResponseError serialization roundtrip`() {
        val error =
            ResponseError(
                code = ResponseError.INTERNAL_ERROR,
                message = "Test error",
                data = buildJsonObject { put("detail", JsonPrimitive("extra info")) },
            )

        val encoded = json.encodeToString(ResponseError.serializer(), error)
        val decoded = json.decodeFromString(ResponseError.serializer(), encoded)

        decoded.code shouldBe error.code
        decoded.message shouldBe error.message
        decoded.data shouldNotBe null
    }

    // ===== MessageSerializer Polymorphic Tests =====

    @Test
    fun `MessageSerializer deserializes request message`() {
        val jsonStr = """{"jsonrpc":"2.0","id":1,"method":"test/method"}"""
        val message = json.decodeFromString(Message.serializer(), jsonStr)
        message.shouldBeInstanceOf<RequestMessage>()
    }

    @Test
    fun `MessageSerializer deserializes notification message`() {
        val jsonStr = """{"jsonrpc":"2.0","method":"test/notification"}"""
        val message = json.decodeFromString(Message.serializer(), jsonStr)
        message.shouldBeInstanceOf<NotificationMessage>()
    }

    @Test
    fun `MessageSerializer deserializes response message with result`() {
        val jsonStr = """{"jsonrpc":"2.0","id":1,"result":"test"}"""
        val message = json.decodeFromString(Message.serializer(), jsonStr)
        message.shouldBeInstanceOf<ResponseMessage>()
    }

    @Test
    fun `MessageSerializer deserializes response message with error`() {
        val jsonStr = """{"jsonrpc":"2.0","id":1,"error":{"code":-32600,"message":"Invalid"}}"""
        val message = json.decodeFromString(Message.serializer(), jsonStr)
        message.shouldBeInstanceOf<ResponseMessage>()
    }

    @Test
    fun `MessageSerializer deserializes response message with null id`() {
        val jsonStr = """{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"Parse error"}}"""
        val message = json.decodeFromString(Message.serializer(), jsonStr)
        val response = message.shouldBeInstanceOf<ResponseMessage>()
        response.id shouldBe null
    }

    @Test
    fun `MessageSerializer deserializes request with string id`() {
        val jsonStr = """{"jsonrpc":"2.0","id":"string-id-123","method":"test"}"""
        val message = json.decodeFromString(Message.serializer(), jsonStr)
        val request = message.shouldBeInstanceOf<RequestMessage>()
        request.id shouldBe RequestId.of("string-id-123")
    }

    @Test
    fun `MessageSerializer throws on invalid message`() {
        val jsonStr = """{"jsonrpc":"2.0"}"""
        assertFailsWith<IllegalArgumentException> {
            json.decodeFromString(Message.serializer(), jsonStr)
        }
    }

    @Test
    fun `MessageSerializer throws on missing jsonrpc version`() {
        val jsonStr = """{"id":1,"method":"test"}"""
        assertFailsWith<IllegalArgumentException> {
            json.decodeFromString(Message.serializer(), jsonStr)
        }
    }

    @Test
    fun `MessageSerializer throws on invalid jsonrpc version`() {
        val jsonStr = """{"jsonrpc":"1.0","id":1,"method":"test"}"""
        assertFailsWith<IllegalArgumentException> {
            json.decodeFromString(Message.serializer(), jsonStr)
        }
    }

    @Test
    fun `MessageSerializer throws on response with both result and error`() {
        val jsonStr = """{"jsonrpc":"2.0","id":1,"result":"test","error":{"code":-32600,"message":"Invalid"}}"""
        assertFailsWith<IllegalArgumentException> {
            json.decodeFromString(Message.serializer(), jsonStr)
        }
    }

    // ===== JSONRPC_VERSION constant Tests =====

    @Test
    fun `JSONRPC_VERSION is 2 point 0`() {
        JSONRPC_VERSION shouldBe "2.0"
    }
}
