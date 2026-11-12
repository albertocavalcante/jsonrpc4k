package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class, kotlinx.serialization.ExperimentalSerializationApi::class)
class InboundResourceLimitsTest {
    private val codec = JsonRpcCodec.Default

    @Test
    fun `frame and nesting limits reject before decoding and reset remains explicit`() {
        val limits = InboundResourceLimits(256, 3, 256)
        val decoder = JsonRpcMessageDecoder(codec, limits)
        val valid = """{"jsonrpc":"2.0","method":"ok","params":[{}]}"""
        assertEquals(1, decoder.feed(frame(valid)).size)
        assertFailsWith<JsonRpcProtocolException> {
            decoder.feed(frame("""{"jsonrpc":"2.0","method":"ok","params":[[{}]]}"""))
        }
        assertFailsWith<JsonRpcProtocolException> { decoder.feed(frame(valid)) }
        decoder.reset()
        assertEquals(1, decoder.feed(frame(valid)).size)
        assertFailsWith<JsonRpcProtocolException> { decoder.feed("Content-Length: 257\r\n\r\n".encodeToByteArray()) }
    }

    @Test
    fun `depth scanner handles escapes lenient literals and fragmented utf8`() {
        val decoder = JsonRpcMessageDecoder(codec, InboundResourceLimits(4096, 2, 4096))
        val body = """{jsonrpc:"2.0",method:ab//cd,params:{url:"http://host",text:"\\\"[{世界}]"}}"""
        val bytes = frame(body)
        val messages = bytes.flatMap { decoder.feed(byteArrayOf(it)) }
        assertEquals(1, messages.size)
        assertIs<NotificationMessage>(messages.single())
    }

    @Test
    fun `large shallow multibyte frames pass exact byte ceiling when fragmented and coalesced`() =
        runTest {
            val payload = JsonPrimitive("世界🌍\\\"".repeat(4_096))
            val message = NotificationMessage("large", payload)
            val bodyBytes = codec.encodeToJson(message).encodeToByteArray().size
            assertTrue(bodyBytes in 32 * 1024..64 * 1024)
            val frame = codec.encode(message)
            val connection =
                Connection(BoundedDispatchPolicy(1, 2), InboundResourceLimits(bodyBytes, 2, bodyBytes * 2), scope = backgroundScope)
            val delivered = Channel<JsonElement?>(Channel.UNLIMITED)
            connection.onNotification("large") { value: JsonElement? -> delivered.send(value) }
            var offset = 0
            while (offset < frame.size) {
                val end = minOf(offset + 2_047, frame.size)
                connection.receive(frame.copyOfRange(offset, end))
                offset = end
            }
            assertEquals(payload, withTimeout(1.seconds) { delivered.receive() })
            runCurrent()
            assertEquals(0, connection.retainedApplicationBytes)
            connection.receive(frame + frame)
            repeat(2) { assertEquals(payload, withTimeout(1.seconds) { delivered.receive() }) }
            connection.close()
        }

    @Test
    fun `depth scanner skips enabled comments but not unquoted slash sequences`() {
        val commentsCodec =
            JsonRpcCodec(
                Json {
                    isLenient = true
                    ignoreUnknownKeys = true
                    allowComments = true
                },
            )
        val decoder = JsonRpcMessageDecoder(commentsCodec, InboundResourceLimits(4096, 2, 4096))
        val valid = """{ /* [{[ */ jsonrpc:"2.0",method:ab//cd,params:{value:"ok"}}"""
        assertEquals(1, decoder.feed(frame(valid)).size)
        assertFailsWith<JsonRpcProtocolException> {
            decoder.feed(frame("""{jsonrpc:"2.0",method:ab//cd,params:{value:[]}}"""))
        }
    }

    @Test
    fun `carriage return inside line comment cannot hide nested structure`() {
        val commentsCodec =
            JsonRpcCodec(
                Json {
                    isLenient = true
                    ignoreUnknownKeys = true
                    allowComments = true
                },
            )
        val decoder = JsonRpcMessageDecoder(commentsCodec, InboundResourceLimits(4096, 2, 4096))
        val body = "{\"jsonrpc\":\"2.0\",\"method\":\"x\",\"params\":{ //comment\r}}}\n\"value\":[]}}"
        assertIs<NotificationMessage>(commentsCodec.decodeFromJson(body))
        assertFailsWith<JsonRpcProtocolException> { decoder.feed(frame(body)) }
    }

    @Test
    fun `unquoted slash prefix is not a comment when comments are disabled`() {
        for (method in listOf("//valid", "/*valid")) {
            val body = """{jsonrpc:"2.0",method:$method,params:{value:[]}}"""
            assertIs<NotificationMessage>(codec.decodeFromJson(body))
            val decoder = JsonRpcMessageDecoder(codec, InboundResourceLimits(4096, 2, 4096))
            assertFailsWith<JsonRpcProtocolException> { decoder.feed(frame(body)) }
        }
    }

    @Test
    fun `application body budget rejects request while running work owns bytes`() =
        runTest {
            val first = RequestMessage(RequestId.of(1), "hold", JsonPrimitive("payload"))
            val second = RequestMessage(RequestId.of(2), "hold", JsonPrimitive("payload"))
            val bodyBytes = codec.encodeToJson(first).encodeToByteArray().size
            val limits = InboundResourceLimits(1024, 8, bodyBytes)
            val connection = Connection(BoundedDispatchPolicy(1, 2), limits, scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val writer = backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            connection.onRequest("hold") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                JsonPrimitive("done")
            }
            connection.receive(codec.encode(first))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(second))
            val busy = JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single()
            assertEquals(-32000, assertIs<ResponseMessage>(busy).error?.code)
            release.complete(Unit)
            val response = JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single()
            assertEquals(first.id, assertIs<ResponseMessage>(response).id)
            runCurrent()
            connection.receive(codec.encode(second))
            val retried = JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single()
            assertEquals(second.id, assertIs<ResponseMessage>(retried).id)
            assertTrue(retried.error == null)
            connection.close()
            writer.cancel()
        }

    @Test
    fun `cancel response does not release bytes while handler cleanup retains request`() =
        runTest {
            val first = RequestMessage(RequestId.of(5), "hold", JsonPrimitive("payload"))
            val second = RequestMessage(RequestId.of(6), "next", JsonPrimitive("payload"))
            val bodyBytes = codec.encodeToJson(first).encodeToByteArray().size
            val connection = Connection(BoundedDispatchPolicy(1, 2), InboundResourceLimits(1024, 8, bodyBytes), scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            val entered = CompletableDeferred<Unit>()
            val cleanup = CompletableDeferred<Unit>()
            val writer = backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            connection.onRequest("hold") { _: JsonElement? ->
                entered.complete(Unit)
                try {
                    CompletableDeferred<JsonElement?>().await()
                } finally {
                    withContext(NonCancellable) { cleanup.await() }
                }
            }
            connection.onRequest("next") { _: JsonElement? -> JsonPrimitive("done") }
            connection.receive(codec.encode(first))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(
                codec.encode(
                    NotificationMessage(
                        "$/cancelRequest",
                        kotlinx.serialization.json.buildJsonObject {
                            put("id", JsonPrimitive(5))
                        },
                    ),
                ),
            )
            val cancelled =
                assertIs<ResponseMessage>(JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single())
            assertEquals(ResponseError.REQUEST_CANCELLED, cancelled.error?.code)
            connection.receive(codec.encode(second))
            val busy = assertIs<ResponseMessage>(JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single())
            assertEquals(-32000, busy.error?.code)
            cleanup.complete(Unit)
            runCurrent()
            connection.receive(codec.encode(second))
            val retried = assertIs<ResponseMessage>(JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single())
            assertEquals(second.id, retried.id)
            assertEquals(null, retried.error)
            connection.close()
            writer.cancel()
        }

    @Test
    fun `responses bypass occupied application byte budget`() =
        runTest {
            val first = RequestMessage(RequestId.of(5), "hold", JsonPrimitive("payload"))
            val bodyBytes = codec.encodeToJson(first).encodeToByteArray().size
            val connection = Connection(BoundedDispatchPolicy(1, 2), InboundResourceLimits(1024, 8, bodyBytes), scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            val entered = CompletableDeferred<Unit>()
            val writer = backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            connection.onRequest("hold") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<JsonElement?>().await()
            }
            connection.receive(codec.encode(first))
            withTimeout(1.seconds) { entered.await() }
            val pending = backgroundScope.async { connection.request<JsonElement>("reverse", null) }
            val outbound = assertIs<RequestMessage>(JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single())
            connection.receive(codec.encode(ResponseMessage.success(outbound.id, JsonPrimitive("ok"))))
            assertEquals(JsonPrimitive("ok"), withTimeout(1.seconds) { pending.await() })
            connection.close()
            writer.cancel()
        }

    @Test
    fun `queued cancellation response does not keep completed work charged`() =
        runTest {
            val first = RequestMessage(RequestId.of(5), "hold", JsonPrimitive("payload"))
            val second = RequestMessage(RequestId.of(6), "next", JsonPrimitive("payload"))
            val bodyBytes = codec.encodeToJson(first).encodeToByteArray().size
            val connection = Connection(BoundedDispatchPolicy(1, 2), InboundResourceLimits(1024, 8, bodyBytes), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val cleanup = CompletableDeferred<Unit>()
            val nextEntered = CompletableDeferred<Unit>()
            connection.onRequest("hold") { _: JsonElement? ->
                entered.complete(Unit)
                try {
                    CompletableDeferred<JsonElement?>().await()
                } finally {
                    withContext(NonCancellable) { cleanup.await() }
                }
            }
            connection.onRequest("next") { _: JsonElement? ->
                nextEntered.complete(Unit)
                null
            }
            connection.receive(codec.encode(first))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(NotificationMessage("$/cancelRequest", buildJsonObject { put("id", JsonPrimitive(5)) })))
            cleanup.complete(Unit)
            runCurrent()
            assertEquals(0, connection.retainedApplicationBytes)
            connection.receive(codec.encode(second))
            withTimeout(1.seconds) { nextEntered.await() }
            assertIs<ConnectionState.Open>(connection.state.value)
            connection.close()
        }

    @Test
    fun `canceled queued request keeps its byte charge until notification barrier passes`() =
        runTest {
            val first = RequestMessage(RequestId.of(5), "hold", JsonPrimitive("a"))
            val barrier = NotificationMessage("barrier", JsonPrimitive("b"))
            val queued = RequestMessage(RequestId.of(6), "hold", JsonPrimitive("c"))

            fun weight(message: Message) = codec.encodeToJson(message).encodeToByteArray().size
            val total = weight(first) + weight(barrier) + weight(queued)
            val connection = Connection(BoundedDispatchPolicy(1, 3), InboundResourceLimits(1024, 8, total), scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val writer = backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            connection.onRequest("hold") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                null
            }
            connection.onNotification("barrier") { _: JsonElement? -> }
            connection.receive(codec.encode(first))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(barrier))
            connection.receive(codec.encode(queued))
            connection.receive(codec.encode(NotificationMessage("$/cancelRequest", buildJsonObject { put("id", JsonPrimitive(6)) })))
            val cancelled =
                assertIs<ResponseMessage>(JsonRpcMessageDecoder(codec).feed(withTimeout(1.seconds) { frames.receive() }).single())
            assertEquals(ResponseError.REQUEST_CANCELLED, cancelled.error?.code)
            assertEquals(total, connection.retainedApplicationBytes)
            release.complete(Unit)
            withTimeout(1.seconds) { frames.receive() }
            runCurrent()
            assertEquals(0, connection.retainedApplicationBytes)
            connection.close()
            writer.cancel()
        }

    @Test
    fun `notification over byte budget terminates the connection`() =
        runTest {
            val notification = NotificationMessage("large", JsonPrimitive("payload"))
            val bodyBytes = codec.encodeToJson(notification).encodeToByteArray().size
            val connection = Connection(BoundedDispatchPolicy(1, 2), InboundResourceLimits(1024, 8, bodyBytes - 1), scope = backgroundScope)
            assertFailsWith<JsonRpcProtocolException> { connection.receive(codec.encode(notification)) }
            assertIs<ConnectionState.Failed>(connection.state.value)
            assertEquals(0, connection.retainedApplicationBytes)
        }

    @Test
    fun `close releases queued events while running cleanup retains its charge`() =
        runTest {
            val first = RequestMessage(RequestId.of(5), "hold", JsonPrimitive("a"))
            val next = RequestMessage(RequestId.of(6), "hold", JsonPrimitive("b"))
            val bodyBytes = codec.encodeToJson(first).encodeToByteArray().size
            val connection = Connection(BoundedDispatchPolicy(1, 2), InboundResourceLimits(1024, 8, bodyBytes * 2), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val cleanup = CompletableDeferred<Unit>()
            connection.onRequest("hold") { _: JsonElement? ->
                entered.complete(Unit)
                try {
                    CompletableDeferred<JsonElement?>().await()
                } finally {
                    withContext(NonCancellable) { cleanup.await() }
                }
            }
            connection.receive(codec.encode(first))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(next))
            assertEquals(bodyBytes * 2, connection.retainedApplicationBytes)
            connection.close()
            runCurrent()
            assertEquals(bodyBytes, connection.retainedApplicationBytes)
            cleanup.complete(Unit)
            runCurrent()
            assertEquals(0, connection.retainedApplicationBytes)
        }

    private fun frame(body: String): ByteArray {
        val bytes = body.encodeToByteArray()
        return "Content-Length: ${bytes.size}\r\n\r\n".encodeToByteArray() + bytes
    }
}
