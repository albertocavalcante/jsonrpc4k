package io.jsonrpc4k.core

import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Encodes a message using the fixed compact default JSON configuration after making a bounded
 * snapshot. Limits apply per invocation; callers must bound concurrent invocations separately.
 * This bounds input to the serializer by wire-size lower bounds, not exact heap use. Serializer
 * expansion, temporary copies and pooling are additional. Caller-owned trees, typed conversion
 * before this API and downstream buffers are outside the bound.
 *
 * Container changes after capture cannot alter the snapshot. Concurrent mutation during capture
 * can fail or produce a mixed view; this API does not provide a coherent concurrent snapshot.
 * Source collection operations must terminate and must not themselves allocate without bound.
 * Raw unquoted JSON literals are not validated as JSON. Quoted strings in valid JSON preserve
 * lone UTF-16 units using escapes before UTF-8 conversion; raw spelling can consequently change.
 * Malformed raw JSON is not repaired and has no quoted-value preservation guarantee.
 * Node accounting treats each raw literal as one primitive; it does not count nodes encoded
 * inside the raw text. The emitted nesting limit is still checked on the resulting bytes.
 *
 * @param maxWireBytes Maximum complete framed bytes and snapshot lower-bound budget, up to 100 MiB.
 * @param maxNodes Maximum charged snapshot nodes, from 1 to 1,000,000.
 * @param maxJsonNesting Maximum emitted object/array nesting, from 1 to 256.
 */
public class BoundedMessageEncoder(
    private val maxWireBytes: Int,
    private val maxNodes: Int,
    private val maxJsonNesting: Int,
) {
    init {
        require(maxWireBytes in 1..JsonRpcCodec.MAX_CONTENT_LENGTH)
        require(maxNodes in 1..MAX_NODES)
        require(maxJsonNesting in 1..MAX_NESTING)
    }

    /** Returns one complete Content-Length frame or throws if a limit is exceeded. */
    public fun encode(message: Message): ByteArray {
        val meter = SnapshotMeter(maxWireBytes, maxNodes, maxJsonNesting)
        val snapshot = meter.snapshotMessage(message)
        val json = JsonRpcCodec.defaultJson
        val body =
            when (snapshot) {
                is RequestMessage -> json.encodeToString(RequestMessage.serializer(), snapshot)
                is NotificationMessage -> json.encodeToString(NotificationMessage.serializer(), snapshot)
                is ResponseMessage -> json.encodeToString(ResponseMessage.serializer(), snapshot)
            }.escapeLoneJsonSurrogates(maxWireBytes).encodeToByteArray()
        JsonNestingGuard(maxJsonNesting, false).check(body)
        val header = "Content-Length: ${body.size}\r\n\r\n".encodeToByteArray()
        if (body.size > maxWireBytes - header.size) throw JsonRpcProtocolException("Encoded frame exceeds $maxWireBytes bytes")
        return header + body
    }

    private companion object {
        const val MAX_NODES = 1_000_000
        const val MAX_NESTING = 256
    }
}

private class SnapshotMeter(
    private val maximum: Int,
    private val maxNodes: Int,
    private val maxDepth: Int,
) {
    private var cost = 0
    private var nodes = 0

    fun snapshotMessage(message: Message): Message {
        chargeNode()
        chargeField("jsonrpc")
        chargeText(message.jsonrpc)
        return when (message) {
            is RequestMessage -> {
                chargeField("id")
                chargeId(message.id)
                chargeField("method")
                chargeText(message.method)
                if (message.params != null) chargeField("params")
                message.copy(params = snapshot(message.params))
            }
            is NotificationMessage -> {
                chargeField("method")
                chargeText(message.method)
                if (message.params != null) chargeField("params")
                message.copy(params = snapshot(message.params))
            }
            is ResponseMessage -> {
                chargeField("id")
                if (message.id != null) chargeId(message.id) else chargeNull()
                val error = message.error
                if (error != null) {
                    require(message.result == null) { "Response cannot have both result and error" }
                    chargeField("error")
                    if (maxDepth < 2) throw JsonRpcProtocolException("Message exceeds nesting limit")
                    chargeNode()
                    chargeField("code")
                    chargeText(error.code.toString())
                    chargeField("message")
                    chargeText(error.message)
                    if (error.data != null) chargeField("data")
                } else {
                    chargeField("result")
                    if (message.result == null) chargeNull()
                }
                message.copy(result = snapshot(message.result), error = error?.copy(data = snapshot(error.data, envelopeDepth = 2)))
            }
        }
    }

    private fun chargeField(name: String) {
        chargeNode()
        chargeText(name)
    }

    private fun chargeNull() {
        chargeNode()
        charge(4)
    }

    private fun chargeId(id: RequestId) {
        chargeNode()
        when (id) {
            is RequestId.NumberId -> chargeText(id.value.toString())
            is RequestId.StringId -> chargeText(id.value)
        }
    }

    private fun chargeNode() {
        if (nodes == maxNodes) throw JsonRpcProtocolException("Message exceeds node limit")
        nodes++
        charge(1)
    }

    private fun chargeText(text: String) = charge(text.length)

    private fun charge(amount: Int) {
        if (amount > maximum - cost) throw JsonRpcProtocolException("Message exceeds snapshot budget")
        cost += amount
    }

    private sealed interface Frame {
        val source: JsonElement

        fun next(meter: SnapshotMeter): JsonElement?

        fun add(value: JsonElement)

        fun finish(): JsonElement
    }

    private class ArrayFrame(
        override val source: JsonArray,
    ) : Frame {
        private val iterator = source.iterator()
        private val values = mutableListOf<JsonElement>()

        override fun next(meter: SnapshotMeter): JsonElement? = if (iterator.hasNext()) iterator.next() else null

        override fun add(value: JsonElement) {
            values.add(value)
        }

        override fun finish(): JsonElement = JsonArray(values.toList())
    }

    private class ObjectFrame(
        override val source: JsonObject,
    ) : Frame {
        private val iterator = source.entries.iterator()
        private val values = linkedMapOf<String, JsonElement>()
        private var key: String? = null

        override fun next(meter: SnapshotMeter): JsonElement? {
            if (!iterator.hasNext()) return null
            val entry = iterator.next()
            val capturedKey = entry.key
            val capturedValue = entry.value
            meter.charge(1)
            meter.chargeText(capturedKey)
            key = capturedKey
            return capturedValue
        }

        override fun add(value: JsonElement) {
            values[checkNotNull(key)] = value
            key = null
        }

        override fun finish(): JsonElement = JsonObject(values.toMap())
    }

    private fun snapshot(
        root: JsonElement?,
        envelopeDepth: Int = 1,
    ): JsonElement? {
        if (root == null) return null
        val stack = mutableListOf<Frame>()
        var current: JsonElement? = root
        var completed: JsonElement? = null
        while (true) {
            if (current != null) {
                chargeNode()
                when (val value = checkNotNull(current)) {
                    is JsonPrimitive -> {
                        chargeText(value.content)
                        completed = value
                    }
                    is JsonArray -> push(stack, ArrayFrame(value), envelopeDepth)
                    is JsonObject -> push(stack, ObjectFrame(value), envelopeDepth)
                }
                current = null
            }
            if (completed != null) {
                if (stack.isEmpty()) return completed
                stack.last().add(completed)
                completed = null
            }
            val frame = stack.last()
            val next = frame.next(this)
            if (next == null) {
                stack.removeAt(stack.lastIndex)
                completed = frame.finish()
            } else {
                current = next
            }
        }
    }

    private fun push(
        stack: MutableList<Frame>,
        frame: Frame,
        envelopeDepth: Int,
    ) {
        if (stack.size + envelopeDepth + 1 > maxDepth) throw JsonRpcProtocolException("Message exceeds nesting limit")
        if (stack.any { it.source === frame.source }) throw JsonRpcProtocolException("Message contains a JSON cycle")
        stack.add(frame)
    }
}
