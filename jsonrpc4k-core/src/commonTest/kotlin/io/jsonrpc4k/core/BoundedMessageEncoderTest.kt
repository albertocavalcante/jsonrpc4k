package io.jsonrpc4k.core

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonUnquotedLiteral
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

@OptIn(ExperimentalSerializationApi::class)
class BoundedMessageEncoderTest {
    private val codec = JsonRpcCodec.Default
    private val encoder = BoundedMessageEncoder(128 * 1024, 10_000, 32)

    @Test
    fun `default wire bytes remain identical for ordinary messages`() {
        val data = JsonObject(mapOf("key" to JsonPrimitive("世界🌍\\\"\n"), "null" to JsonNull))
        val messages =
            listOf(
                RequestMessage(RequestId.of("id🌍"), "m", data),
                RequestMessage(RequestId.of(Long.MIN_VALUE), "negative", null),
                NotificationMessage("n", JsonArray(listOf(data))),
                ResponseMessage.success(RequestId.of(1), null),
                ResponseMessage.success(RequestId.of(2), data),
                ResponseMessage.error(RequestId.of(3), ResponseError(-32000, "problem", data)),
                ResponseMessage(id = null, error = ResponseError.parseError("parse error")),
                ResponseMessage(id = null, error = ResponseError.invalidRequest("invalid request")),
                NotificationMessage("raw", JsonUnquotedLiteral("[{\"a\":1}]")),
                NotificationMessage("raw-invalid", JsonUnquotedLiteral("not-json")),
                NotificationMessage("escaped", JsonObject(mapOf("\\\"\n".repeat(100) to JsonPrimitive("\u0000\n\\\"".repeat(100))))),
                NotificationMessage("surrogate", JsonPrimitive("\uD800")),
            )
        for (message in messages) {
            assertContentEquals(codec.encode(message), encoder.encode(message))
        }
    }

    @Test
    fun `response with both result and error is rejected`() {
        val invalid = ResponseMessage(RequestId.of(1), JsonPrimitive("result"), ResponseError(-32000, "error"))
        assertFailsWith<IllegalArgumentException> { encoder.encode(invalid) }
    }

    @Test
    fun `exact frame ceiling is accepted and one byte less rejects`() {
        val message = NotificationMessage("large", JsonPrimitive("世界🌍\\\"".repeat(4_096)))
        val expected = codec.encode(message)
        assertTrue(expected.size in 32 * 1024..64 * 1024)
        assertContentEquals(expected, BoundedMessageEncoder(expected.size, 100, 8).encode(message))
        assertFailsWith<JsonRpcProtocolException> { BoundedMessageEncoder(expected.size - 1, 100, 8).encode(message) }
    }

    @Test
    fun `node depth oversized and cyclic inputs fail before serialization`() {
        val nested = JsonArray(listOf(JsonArray(listOf(JsonArray(listOf(JsonPrimitive(1)))))))
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(1024, 100, 3).encode(NotificationMessage("n", nested))
        }
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(256, 100, 16).encode(NotificationMessage("n", JsonPrimitive("x".repeat(300))))
        }
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(256, 100, 16).encode(NotificationMessage("n", JsonUnquotedLiteral("1".repeat(300))))
        }
        var dag: JsonElement = JsonPrimitive(1)
        repeat(8) { dag = JsonArray(listOf(dag, dag)) }
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(16 * 1024, 50, 16).encode(NotificationMessage("n", dag))
        }
        val map = mutableMapOf<String, JsonElement>()
        val cycle = JsonObject(map)
        map["self"] = cycle
        assertFailsWith<JsonRpcProtocolException> { encoder.encode(NotificationMessage("n", cycle)) }
    }

    @Test
    fun `raw nested literal is checked against emitted nesting`() {
        val literal = JsonUnquotedLiteral("[[[1]]]")
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(1024, 100, 3).encode(NotificationMessage("n", literal))
        }
    }

    @Test
    fun `maximum supported array and object nesting is accepted but one deeper rejects`() {
        for (objects in listOf(false, true)) {
            var value: JsonElement = JsonPrimitive(1)
            repeat(255) {
                value = if (objects) JsonObject(mapOf("v" to value)) else JsonArray(listOf(value))
            }
            val message = NotificationMessage("deep", value)
            val bounded = BoundedMessageEncoder(16 * 1024, 1_000, 256)
            assertContentEquals(codec.encode(message), bounded.encode(message))
            val tooDeep = if (objects) JsonObject(mapOf("v" to value)) else JsonArray(listOf(value))
            assertFailsWith<JsonRpcProtocolException> { bounded.encode(NotificationMessage("deep", tooDeep)) }
        }
    }

    @Test
    fun `captured containers do not reflect later source mutation`() {
        val backing = linkedMapOf<String, JsonElement>("key" to JsonPrimitive("old"))
        val changing =
            object : Map<String, JsonElement> by backing {
                override val entries: Set<Map.Entry<String, JsonElement>>
                    get() =
                        object : AbstractSet<Map.Entry<String, JsonElement>>() {
                            override val size: Int get() = 1

                            override fun iterator(): Iterator<Map.Entry<String, JsonElement>> {
                                var delivered = false
                                return object : Iterator<Map.Entry<String, JsonElement>> {
                                    override fun hasNext(): Boolean {
                                        if (delivered) backing["key"] = JsonPrimitive("new")
                                        return !delivered
                                    }

                                    override fun next(): Map.Entry<String, JsonElement> {
                                        if (delivered) throw NoSuchElementException()
                                        delivered = true
                                        return backing.entries.first()
                                    }
                                }
                            }
                        }
            }
        val bytes = encoder.encode(NotificationMessage("n", JsonObject(changing)))
        val decoded = assertIsNotification(JsonRpcMessageDecoder(codec).feed(bytes).single())
        assertEquals(JsonPrimitive("old"), (decoded.params as JsonObject)["key"])
        assertEquals(JsonPrimitive("new"), backing["key"])
    }

    private fun assertIsNotification(message: Message): NotificationMessage = message as NotificationMessage
}
