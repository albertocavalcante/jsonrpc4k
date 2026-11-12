package io.jsonrpc4k.core

import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
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
            val messages = frame(body).flatMap { decoder.feed(byteArrayOf(it)) }
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
            val decoded = JsonRpcMessageDecoder().feed(bytes).single()
            assertUnits(vector, source(assertIs<NotificationMessage>(decoded)))
        }
    }

    @Test
    fun `bounded framed output preserves source UTF16 units`() {
        val encoder = BoundedMessageEncoder(MAX_WIRE_BYTES, MAX_NODES, MAX_DEPTH)
        forEachVector { vector ->
            val bytes = encoder.encode(message(vector.text))
            assertFrameLength(bytes)
            val decoded = JsonRpcMessageDecoder().feed(bytes).single()
            assertUnits(vector, source(assertIs<NotificationMessage>(decoded)))
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
    }
}
