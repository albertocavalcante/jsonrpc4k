package io.jsonrpc4k.core

import kotlinx.serialization.ExperimentalSerializationApi
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonUnquotedLiteral
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** Wire-level UTF-16 characterization; document-edit semantics are tested by consumers. */
class SourceTextWireTest {
    private data class Vector(
        val name: String,
        val escaped: String,
        val text: String,
    )

    private val vectors =
        listOf(
            Vector("ASCII", "plain", "plain"),
            Vector("astral pair", """\ud83d\ude00""", "\uD83D\uDE00"),
            Vector("lone high", """\ud800""", "\uD800"),
            Vector("lone low", """\udc00""", "\uDC00"),
            Vector("split pair", """\ud83dx\ude00""", "\uD83Dx\uDE00"),
            Vector("replacement character", """\ufffd""", "\uFFFD"),
            Vector("mixed line endings", """a\r\nb\rc\nd""", "a\r\nb\rc\nd"),
            Vector("empty", "", ""),
        )

    @Test
    fun `escaped framed input preserves source UTF16 units`() {
        forEachVector { vector ->
            val body = """{"jsonrpc":"2.0","method":"source/text","params":{"text":"${vector.escaped}"}}"""
            val decoder = JsonRpcMessageDecoder()
            val messages = frame(body).flatMap { decoder.feed(byteArrayOf(it)).messages() }
            assertUnits(vector, source(assertIs<NotificationMessage>(messages.single())))
        }
    }

    @Test
    fun `JSON String output preserves source UTF16 units before framing`() {
        forEachVector { vector ->
            val json = JsonRpcCodec.Default.encodeToJson(message(vector.text))
            val decoded = JsonRpcCodec.Default.decodeFromJson(json)
            assertUnits(vector, source(assertIs<NotificationMessage>(decoded)))
        }
    }

    @Test
    fun `default framed output preserves source UTF16 units`() {
        forEachVector { vector ->
            val bytes = JsonRpcCodec.Default.encode(message(vector.text))
            assertFrameLength(bytes)
            val decoded = JsonRpcMessageDecoder().feed(bytes).messages().single()
            assertUnits(vector, source(assertIs<NotificationMessage>(decoded)))
        }
    }

    @Test
    fun `bounded framed output preserves source UTF16 units`() {
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        forEachVector { vector ->
            val bytes = encoder.encode(message(vector.text))
            assertFrameLength(bytes)
            val decoded = JsonRpcMessageDecoder().feed(bytes).messages().single()
            assertUnits(vector, source(assertIs<NotificationMessage>(decoded)))
        }
    }

    @Test
    fun `quotes and backslashes do not change surrogate boundaries`() {
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        for (prefix in listOf("\"", "\\", "\\\"", "\\\\\"", "\\uD800")) {
            val text = prefix + "\uD800" + prefix + "\uDC00"
            val message = message(text)
            for (bytes in listOf(JsonRpcCodec.Default.encode(message), encoder.encode(message))) {
                val decoded = assertIs<NotificationMessage>(JsonRpcMessageDecoder().feed(bytes).messages().single())
                assertEquals(text.map { it.code }, source(decoded).map { it.code })
            }
        }
    }

    @Test
    fun `keys method request ID and error text preserve lone units`() {
        val high = "\uD800"
        val low = "\uDC00"
        val messages =
            listOf(
                RequestMessage(RequestId.of(high), low, JsonObject(mapOf(high to JsonPrimitive(low)))),
                ResponseMessage.error(RequestId.of(high), ResponseError(-1, low, JsonPrimitive(high))),
            )
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        for (message in messages) {
            for (bytes in listOf(JsonRpcCodec.Default.encode(message), encoder.encode(message))) {
                assertEquals(message, JsonRpcMessageDecoder().feed(bytes).messages().single())
                assertFrameLength(bytes)
            }
        }
    }

    @Test
    fun `escaping expansion is included in exact framed byte limit`() {
        val single = message("\uD800")
        val expected = frame("""{"method":"source/text","params":{"text":"\ud800"},"jsonrpc":"2.0"}""")
        assertContentEquals(expected, JsonRpcCodec.Default.encode(single))
        assertContentEquals(expected, BoundedMessageEncoder(expected.size, MAX_NODES, MAX_DEPTH).encode(single))
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(expected.size - 1, MAX_NODES, MAX_DEPTH).encode(single)
        }
        assertFailsWith<JsonRpcProtocolException> {
            BoundedMessageEncoder(EXPANSION_LIMIT, MAX_NODES, MAX_DEPTH).encode(message("\uD800".repeat(REPEATED_UNITS)))
        }
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Test
    fun `valid raw numbers and quoted values retain parsed values`() {
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        val high = "\uD800"
        val rawValues = listOf("1.2300", "\"$high\"", """{"text":"$high"}""")
        for (raw in rawValues) {
            val message = NotificationMessage("raw", JsonUnquotedLiteral(raw))
            val expected = JsonRpcCodec.defaultJson.parseToJsonElement(raw)
            for (bytes in listOf(JsonRpcCodec.Default.encode(message), encoder.encode(message))) {
                val decoded = assertIs<NotificationMessage>(JsonRpcMessageDecoder().feed(bytes).messages().single())
                assertEquals(expected, decoded.params)
            }
        }
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Test
    fun `malformed raw output remains unvalidated with encoder parity`() {
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        for (raw in listOf("not-json", "\"", "\"\\", "/* \" */")) {
            val params = JsonArray(listOf(JsonUnquotedLiteral(raw), JsonPrimitive("\uD800")))
            val message = NotificationMessage("raw", params)
            assertContentEquals(JsonRpcCodec.Default.encode(message), encoder.encode(message))
        }
    }

    @OptIn(ExperimentalSerializationApi::class)
    @Test
    fun `configured comments containing quotes do not hide source strings`() {
        val json =
            Json(JsonRpcCodec.defaultJson) {
                allowComments = true
                prettyPrint = true
            }
        val codec = JsonRpcCodec(json)
        val high = "\uD800"
        val comments = listOf("/* \" \\ */", "// \" \\\n", "/* /* \" */")
        for (comment in comments) {
            val raw = comment + """{"text":"$high"}"""
            val message = NotificationMessage("raw", JsonUnquotedLiteral(raw))
            val decoded = assertIs<NotificationMessage>(JsonRpcMessageDecoder(codec).feed(codec.encode(message)).messages().single())
            assertEquals(high.map { it.code }, source(decoded).map { it.code })
        }
    }

    @Test
    fun `well-formed text retains original framing bytes`() {
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        for (text in listOf("", "ascii", "\uD83D\uDE00", "\uFFFD", "a\r\n\\\"")) {
            val message = message(text)
            val expected = frame(JsonRpcCodec.Default.encodeToJson(message))
            assertContentEquals(expected, JsonRpcCodec.Default.encode(message))
            assertContentEquals(expected, encoder.encode(message))
        }
    }

    private fun message(text: String): NotificationMessage =
        NotificationMessage("source/text", JsonObject(mapOf("text" to JsonPrimitive(text))))

    private fun source(message: NotificationMessage): String =
        assertIs<JsonPrimitive>(assertIs<JsonObject>(message.params).getValue("text")).content

    private fun assertUnits(
        vector: Vector,
        actual: String,
    ) {
        assertEquals(vector.text.map { it.code }, actual.map { it.code }, vector.name)
    }

    private fun forEachVector(check: (Vector) -> Unit) {
        val failures = mutableListOf<String>()
        for (vector in vectors) {
            try {
                check(vector)
            } catch (failure: AssertionError) {
                failures.add("${vector.name}: ${failure.message}")
            }
        }
        assertTrue(failures.isEmpty(), failures.joinToString("\n"))
    }

    private fun frame(body: String): ByteArray {
        val bytes = body.encodeToByteArray()
        return "Content-Length: ${bytes.size}\r\n\r\n".encodeToByteArray() + bytes
    }

    private fun assertFrameLength(bytes: ByteArray) {
        val wire = bytes.decodeToString(throwOnInvalidSequence = true)
        val header = wire.substringBefore("\r\n\r\n")
        val body = wire.substringAfter("\r\n\r\n").encodeToByteArray()
        assertEquals(body.size, header.substringAfter("Content-Length: ").toInt())
    }

    private companion object {
        const val MAX_WIRE_BYTES = 4096
        const val MAX_NODES = 128
        const val MAX_DEPTH = 16
        const val EXPANSION_LIMIT = 256
        const val REPEATED_UNITS = 64
    }
}
