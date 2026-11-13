package io.jsonrpc4k.core

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs

/** JSON-RPC 2.0 batch handling (specification section 6) in the codec and in every dispatch mode. */
class JsonRpcBatchTest {
    private val codec = JsonRpcCodec.Default

    @Test
    fun `empty array decodes to a single Invalid Request`() {
        assertEquals(Payload.Invalid.INVALID_REQUEST, codec.decodePayload("[]"))
        assertEquals(Payload.Invalid.INVALID_REQUEST, codec.decodePayload(" [ ] "))
    }

    @Test
    fun `batch keeps invalid entries as null in wire order`() {
        val payload =
            codec.decodePayload(
                """[{"jsonrpc":"2.0","method":"a"},1,{"jsonrpc":"2.0","method":7,"id":1},""" +
                    """{"foo":"boo"},{"jsonrpc":"2.0","result":2,"id":3}]""",
            )
        val response = ResponseMessage.success(RequestId.of(3), JsonPrimitive(2))
        assertEquals(Payload.Batch(listOf(NotificationMessage("a"), null, null, null, response)), payload)
    }

    @Test
    fun `invalid JSON is a parse error and invalid objects are Invalid Request`() {
        assertEquals(Payload.Invalid.PARSE_ERROR, codec.decodePayload("""{"jsonrpc":"2.0","method":"""))
        assertEquals(Payload.Invalid.PARSE_ERROR, codec.decodePayload(""))
        assertEquals(Payload.Invalid.INVALID_REQUEST, codec.decodePayload("42"))
        assertEquals(Payload.Invalid.INVALID_REQUEST, codec.decodePayload("""{"jsonrpc":"2.0","method":1,"params":"bar"}"""))
        assertEquals(Payload.Invalid.INVALID_REQUEST, codec.decodePayload("""{"jsonrpc":2.0,"method":"a"}"""))
        // The default codec is lenient: unquoted text that is not a JSON literal still reads as a string.
        assertEquals(Payload.Single(NotificationMessage("ab")), codec.decodePayload("""{jsonrpc:"2.0",method:ab}"""))
    }

    @Test
    fun `batch encodes as one JSON array and is never empty`() {
        val responses = listOf(ResponseMessage.success(RequestId.of(1), JsonPrimitive(3)), Payload.Invalid.INVALID_REQUEST.response)
        val expected =
            """[{"jsonrpc":"2.0","id":1,"result":3},""" +
                """{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"Invalid Request"}}]"""
        assertEquals(Json.parseToJsonElement(expected), Json.parseToJsonElement(codec.encodeToJson(responses)))
        assertFailsWith<IllegalArgumentException> { codec.encodeToJson(emptyList()) }
        val encoder = BoundedMessageEncoder(maxWireBytes = 4096, maxNodes = 100, maxJsonNesting = 4)
        assertEquals(codec.encode(responses).decodeToString(), encoder.encode(responses).decodeToString())
        assertFailsWith<IllegalArgumentException> { encoder.encode(emptyList()) }
    }

    @Test
    fun `empty array is answered with one Invalid Request object`() =
        runTest {
            for (connect in dispatchModes) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                connection.receive(wireFrame("[]"))
                val reply = assertIs<JsonObject>(wireJson(frames.nextFrame()))
                assertEquals(JsonNull, reply["id"])
                assertEquals(ResponseError.INVALID_REQUEST, reply.errorCode())
                connection.close()
            }
        }

    @Test
    fun `all-notification batch runs every notification and sends nothing`() =
        runTest {
            for (connect in dispatchModes) {
                val received = mutableListOf<String>()
                val connection = connect(backgroundScope)
                connection.onNotification("first") { _: JsonElement? -> received.add("first") }
                connection.onNotification("second") { _: JsonElement? -> received.add("second") }
                connection.onRequest("ping") { _: JsonElement? -> JsonPrimitive("pong") }
                val frames = backgroundScope.outgoingFrames(connection)
                connection.receive(wireFrame("""[{"jsonrpc":"2.0","method":"first"},{"jsonrpc":"2.0","method":"second"}]"""))
                connection.receive(wireFrame("""{"jsonrpc":"2.0","method":"ping","id":1}"""))
                // The ping reply is the next frame, so the batch sent nothing.
                assertEquals(Json.parseToJsonElement("""{"jsonrpc":"2.0","id":1,"result":"pong"}"""), wireJson(frames.nextFrame()))
                assertEquals(listOf("first", "second"), received)
                connection.close()
            }
        }

    @Test
    fun `mixed batch answers its requests and invalid entries in one array`() =
        runTest {
            for (connect in dispatchModes) {
                val received = mutableListOf<JsonElement?>()
                val connection = connect(backgroundScope)
                connection.onRequest("echo") { params: JsonElement? -> params }
                connection.onNotification("note") { params: JsonElement? -> received.add(params) }
                val frames = backgroundScope.outgoingFrames(connection)
                connection.receive(wireFrame(MIXED_BATCH))
                val reply = assertIs<JsonArray>(wireJson(frames.nextFrame()))
                val byId = reply.associateBy { it.jsonObject.getValue("id") }
                assertEquals(setOf(JsonPrimitive(1), JsonNull, JsonPrimitive("m")), byId.keys)
                assertEquals(3, reply.size)
                assertEquals(JsonArray(listOf(JsonPrimitive("a"))), byId.getValue(JsonPrimitive(1)).jsonObject["result"])
                assertEquals(ResponseError.INVALID_REQUEST, byId.getValue(JsonNull).errorCode())
                assertEquals(ResponseError.METHOD_NOT_FOUND, byId.getValue(JsonPrimitive("m")).errorCode())
                assertEquals(listOf<JsonElement?>(JsonArray(listOf(JsonPrimitive(1)))), received)
                connection.close()
            }
        }

    private fun JsonElement.errorCode(): Int {
        val error = jsonObject.getValue("error").jsonObject
        return error.getValue("code").jsonPrimitive.int
    }

    private companion object {
        /** A request, a notification, an invalid entry, an unknown method, a stray response and a stray cancellation. */
        const val MIXED_BATCH =
            """[{"jsonrpc":"2.0","method":"echo","params":["a"],"id":1},{"jsonrpc":"2.0","method":"note","params":[1]},7,""" +
                """{"jsonrpc":"2.0","method":"missing","id":"m"},{"jsonrpc":"2.0","result":0,"id":42},""" +
                """{"jsonrpc":"2.0","method":"$/cancelRequest","params":{"id":99}}]"""
    }
}
