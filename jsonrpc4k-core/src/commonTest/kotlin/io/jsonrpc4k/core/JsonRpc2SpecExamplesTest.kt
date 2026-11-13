package io.jsonrpc4k.core

import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Executable conformance suite: the twelve examples in section 7 of the JSON-RPC 2.0 specification,
 * transcribed from protocol4k-plan `specs/jsonrpc-2.0/examples.json` (page sha256 8fe1edfd...aefeb).
 * Only the line breaks and indentation between JSON tokens are dropped; every token is verbatim.
 *
 * Every example runs on a fresh connection in each dispatch mode. Responses compare as parsed JSON.
 * Batch arrays compare as multisets because the specification allows any order, and error objects
 * compare by code because their message text is not normative (section 5.1). After each example a
 * sentinel request must be answered next, which proves that nothing else was sent.
 */
class JsonRpc2SpecExamplesTest {
    private class Step(
        val request: String,
        val response: String? = null,
    )

    private class Example(
        val title: String,
        val steps: List<Step>,
    )

    @Test
    fun `specification examples hold in every dispatch mode`() =
        runTest {
            for ((mode, connect) in dispatchModes.withIndex()) {
                for (example in EXAMPLES) {
                    val connection = connect(backgroundScope)
                    registerHandlers(connection)
                    assertExample("mode $mode, ${example.title}", example, connection, backgroundScope.outgoingFrames(connection))
                    connection.close()
                }
            }
        }

    private suspend fun assertExample(
        label: String,
        example: Example,
        connection: Connection,
        frames: ReceiveChannel<ByteArray>,
    ) {
        for (step in example.steps) {
            connection.receive(wireFrame(step.request))
            step.response?.let { assertReply(label, it, frames.nextFrame()) }
        }
        connection.receive(wireFrame(SENTINEL_REQUEST))
        assertReply("$label: the sentinel reply must come next", SENTINEL_RESPONSE, frames.nextFrame())
    }

    private fun assertReply(
        label: String,
        expected: String,
        frame: ByteArray,
    ) {
        assertEquals(normalize(Json.parseToJsonElement(expected)), normalize(wireJson(frame)), label)
    }

    /** A batch array becomes a multiset of its responses; a single response stays an object. */
    private fun normalize(reply: JsonElement): Any =
        if (reply is JsonArray) reply.map { normalizeResponse(it) }.groupingBy { it }.eachCount() else normalizeResponse(reply)

    private fun normalizeResponse(element: JsonElement): JsonElement {
        val response = element.jsonObject
        val error = response["error"]?.jsonObject ?: return response
        assertTrue(error["message"]?.jsonPrimitive?.isString == true, "error message must be a string: $error")
        return JsonObject(response + ("error" to JsonObject(mapOf("code" to error.getValue("code")))))
    }

    private suspend fun registerHandlers(connection: Connection) {
        connection.onRequest("subtract") { params: JsonElement? -> JsonPrimitive(subtract(params)) }
        connection.onRequest("sum") { params: JsonElement? -> JsonPrimitive(sum(params)) }
        connection.onRequest("get_data") { _: JsonElement? -> JsonArray(listOf(JsonPrimitive("hello"), JsonPrimitive(5))) }
        for (method in listOf("update", "foobar", "notify_hello", "notify_sum")) {
            connection.onNotification(method) { _: JsonElement? -> }
        }
    }

    private fun sum(params: JsonElement?): Int = checkNotNull(params).jsonArray.sumOf { it.jsonPrimitive.int }

    private fun subtract(params: JsonElement?): Int =
        when (params) {
            is JsonArray -> params[0].jsonPrimitive.int - params[1].jsonPrimitive.int
            is JsonObject -> params.getValue("minuend").jsonPrimitive.int - params.getValue("subtrahend").jsonPrimitive.int
            else -> throw JsonRpcException.invalidParams("subtract takes two numbers")
        }

    private companion object {
        const val SENTINEL_REQUEST = """{"jsonrpc": "2.0", "method": "sum", "params": [1, 2], "id": "sentinel"}"""
        const val SENTINEL_RESPONSE = """{"jsonrpc": "2.0", "result": 3, "id": "sentinel"}"""

        val EXAMPLES: List<Example> =
            listOf(
                Example(
                    "rpc call with positional parameters",
                    listOf(
                        Step(
                            """{"jsonrpc": "2.0", "method": "subtract", "params": [42, 23], "id": 1}""",
                            """{"jsonrpc": "2.0", "result": 19, "id": 1}""",
                        ),
                        Step(
                            """{"jsonrpc": "2.0", "method": "subtract", "params": [23, 42], "id": 2}""",
                            """{"jsonrpc": "2.0", "result": -19, "id": 2}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call with named parameters",
                    listOf(
                        Step(
                            """{"jsonrpc": "2.0", "method": "subtract", "params": {"subtrahend": 23, "minuend": 42}, "id": 3}""",
                            """{"jsonrpc": "2.0", "result": 19, "id": 3}""",
                        ),
                        Step(
                            """{"jsonrpc": "2.0", "method": "subtract", "params": {"minuend": 42, "subtrahend": 23}, "id": 4}""",
                            """{"jsonrpc": "2.0", "result": 19, "id": 4}""",
                        ),
                    ),
                ),
                Example(
                    "a notification",
                    listOf(
                        Step(
                            """{"jsonrpc": "2.0", "method": "update", "params": [1,2,3,4,5]}""",
                        ),
                        Step(
                            """{"jsonrpc": "2.0", "method": "foobar"}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call of non-existent method",
                    listOf(
                        Step(
                            """{"jsonrpc": "2.0", "method": "foobar", "id": "1"}""",
                            """{"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": "1"}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call with invalid JSON",
                    listOf(
                        Step(
                            """{"jsonrpc": "2.0", "method": "foobar, "params": "bar", "baz]""",
                            """{"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": null}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call with invalid Request object",
                    listOf(
                        Step(
                            """{"jsonrpc": "2.0", "method": 1, "params": "bar"}""",
                            """{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call Batch, invalid JSON",
                    listOf(
                        Step(
                            """[{"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},{"jsonrpc": "2.0", "method"]""",
                            """{"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": null}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call with an empty Array",
                    listOf(
                        Step(
                            """[]""",
                            """{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}""",
                        ),
                    ),
                ),
                Example(
                    "rpc call with an invalid Batch (but not empty)",
                    listOf(
                        Step(
                            """[1]""",
                            """[{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}]""",
                        ),
                    ),
                ),
                Example(
                    "rpc call with invalid Batch",
                    listOf(
                        Step(
                            """[1,2,3]""",
                            """[{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null},""" +
                                """{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null},""" +
                                """{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}]""",
                        ),
                    ),
                ),
                Example(
                    "rpc call Batch",
                    listOf(
                        Step(
                            """[{"jsonrpc": "2.0", "method": "sum", "params": [1,2,4], "id": "1"},""" +
                                """{"jsonrpc": "2.0", "method": "notify_hello", "params": [7]},""" +
                                """{"jsonrpc": "2.0", "method": "subtract", "params": [42,23], "id": "2"},{"foo": "boo"},""" +
                                """{"jsonrpc": "2.0", "method": "foo.get", "params": {"name": "myself"}, "id": "5"},""" +
                                """{"jsonrpc": "2.0", "method": "get_data", "id": "9"}]""",
                            """[{"jsonrpc": "2.0", "result": 7, "id": "1"},{"jsonrpc": "2.0", "result": 19, "id": "2"},""" +
                                """{"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null},""" +
                                """{"jsonrpc": "2.0", "error": {"code": -32601, "message": "Method not found"}, "id": "5"},""" +
                                """{"jsonrpc": "2.0", "result": ["hello", 5], "id": "9"}]""",
                        ),
                    ),
                ),
                Example(
                    "rpc call Batch (all notifications)",
                    listOf(
                        Step(
                            """[{"jsonrpc": "2.0", "method": "notify_sum", "params": [1,2,4]},""" +
                                """{"jsonrpc": "2.0", "method": "notify_hello", "params": [7]}]""",
                        ),
                    ),
                ),
            )
    }
}
