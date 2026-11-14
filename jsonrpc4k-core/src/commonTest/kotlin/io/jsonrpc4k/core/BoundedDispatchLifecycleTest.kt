package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.currentTime
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** End-of-input drain, per-peer cancellation, cancellable batch entries and the control-notification lane. */
@OptIn(ExperimentalCoroutinesApi::class)
class BoundedDispatchLifecycleTest {
    private val codec = JsonRpcCodec.Default

    @Test
    fun `policy rejects a negative or infinite drain timeout`() {
        assertFailsWith<IllegalArgumentException> { BoundedDispatchPolicy(1, 1, drainTimeout = (-1).milliseconds) }
        assertFailsWith<IllegalArgumentException> { BoundedDispatchPolicy(1, 1, drainTimeout = Duration.INFINITE) }
        assertEquals(5.seconds, BoundedDispatchPolicy(1, 1).drainTimeout)
        assertEquals(Duration.ZERO, BoundedDispatchPolicy(1, 1, drainTimeout = Duration.ZERO).drainTimeout)
    }

    @Test
    fun `a request answered after the peer closed its output still gets its response`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(2, 4), scope = backgroundScope)
            val frames = backgroundScope.outgoingFrames(connection)
            connection.onRequest("slow") { _: JsonElement? ->
                delay(500.milliseconds)
                JsonPrimitive("done")
            }
            // The peer sends its last request and closes its output at once.
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "slow")))
            val ended = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.endInput() }
            runCurrent()
            assertFalse(ended.isCompleted, "the drain must wait for the running handler")
            val response = assertIs<ResponseMessage>(decode(frames.nextFrame()))
            assertEquals(JsonPrimitive("done"), response.result)
            ended.await()
            assertEquals(500L, currentTime)
            assertEquals(ConnectionState.Open, connection.state.value)
            assertFailsWith<IllegalStateException> { connection.receive(codec.encode(RequestMessage(RequestId.of(2), "slow"))) }
            assertEquals(ConnectionState.Open, connection.state.value)
            connection.close()
        }

    @Test
    fun `end of input runs queued notifications and batches before returning`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 8), scope = backgroundScope)
            val frames = backgroundScope.outgoingFrames(connection)
            val order = mutableListOf<String>()
            connection.onRequest("work") { params: JsonElement? ->
                delay(100.milliseconds)
                order.add("work $params")
                params
            }
            connection.onNotification("note") { _: JsonElement? -> order.add("note") }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "work", JsonPrimitive(1))))
            connection.receive(codec.encode(NotificationMessage("note")))
            connection.receive(
                wireFrame("""[{"jsonrpc":"2.0","method":"work","params":2,"id":2},{"jsonrpc":"2.0","method":"work","params":3,"id":3}]"""),
            )
            connection.endInput()
            assertEquals(listOf("work 1", "note", "work 2", "work 3"), order)
            assertEquals(JsonPrimitive(1), assertIs<ResponseMessage>(decode(frames.nextFrame())).result)
            val batch = assertIs<JsonArray>(wireJson(frames.nextFrame()))
            assertEquals(
                listOf(2, 3),
                batch.map {
                    it.jsonObject
                        .getValue("result")
                        .jsonPrimitive.int
                },
            )
            connection.close()
        }

    @Test
    fun `drain timeout cancels the remaining handlers and answers them as cancelled`() =
        runTest {
            val policy = BoundedDispatchPolicy(1, 4, drainTimeout = 2.seconds)
            val connection = Connection(policy, scope = backgroundScope)
            val frames = backgroundScope.outgoingFrames(connection)
            val entered = CompletableDeferred<Unit>()
            val observed = CompletableDeferred<Unit>()
            var queuedStarted = false
            connection.onRequest("stuck", runUntilCancelled(entered, observed))
            connection.onRequest("queued") { _: JsonElement? ->
                queuedStarted = true
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "stuck")))
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "queued")))
            withTimeout(1.seconds) { entered.await() }
            val start = currentTime
            connection.endInput()
            assertEquals(2_000L, currentTime - start)
            withTimeout(1.seconds) { observed.await() }
            val answered = List(2) { assertIs<ResponseMessage>(decode(frames.nextFrame())) }
            assertEquals(setOf(RequestId.of(1), RequestId.of(2)), answered.map { it.id }.toSet())
            assertTrue(answered.all { it.error?.code == ResponseError.REQUEST_CANCELLED })
            runCurrent()
            assertTrue(frames.isEmpty)
            assertFalse(queuedStarted)
            assertEquals(ConnectionState.Open, connection.state.value)
            // A later call does not wait for the timeout again.
            val again = currentTime
            connection.endInput()
            assertEquals(again, currentTime)
            connection.close()
        }

    @Test
    fun `drain timeout still sends a batch as one array with the stuck entry cancelled`() =
        runTest {
            for (connect in boundedModesWith(BoundedDispatchPolicy(2, 8, drainTimeout = 2.seconds))) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                val entered = CompletableDeferred<Unit>()
                val observed = CompletableDeferred<Unit>()
                val notes = mutableListOf<JsonElement?>()
                connection.onRequest("slow", runUntilCancelled(entered, observed))
                connection.onRequest("echo") { params: JsonElement? -> params }
                connection.onNotification("note") { params: JsonElement? -> notes.add(params) }
                connection.receive(wireFrame(STUCK_BATCH))
                withTimeout(1.seconds) { entered.await() }
                runCurrent()
                assertTrue(frames.isEmpty, "the array waits for the stuck entry")
                val start = currentTime
                connection.endInput()
                assertEquals(2_000L, currentTime - start)
                withTimeout(1.seconds) { observed.await() }
                val reply = assertIs<JsonArray>(wireJson(frames.nextFrame()))
                val byId = reply.associateBy { it.jsonObject.getValue("id") }
                assertEquals(4, reply.size)
                assertEquals(setOf(JsonPrimitive(1), JsonPrimitive("slow"), JsonPrimitive(2), JsonNull), byId.keys)
                assertEquals(JsonPrimitive("a"), byId.getValue(JsonPrimitive(1)).jsonObject["result"])
                assertEquals(ResponseError.REQUEST_CANCELLED, byId.getValue(JsonPrimitive("slow")).errorCode())
                assertEquals(JsonPrimitive("b"), byId.getValue(JsonPrimitive(2)).jsonObject["result"])
                assertEquals(ResponseError.INVALID_REQUEST, byId.getValue(JsonNull).errorCode())
                assertEquals(listOf<JsonElement?>(JsonPrimitive("n")), notes)
                connection.flushOutgoing()
                runCurrent()
                assertTrue(frames.isEmpty, "a batch is answered with exactly one frame")
                assertEquals(ConnectionState.Open, connection.state.value)
                connection.close()
            }
        }

    // A contract test for the documented shutdown order. Its collector writes without suspending, so
    // under runTest's FIFO dispatcher it also passes when endInput returns before the array is queued:
    // the collector takes the array right after completing the barrier, before the test resumes. The
    // next test, whose collector suspends while writing, is the one that needs the handoff wait.
    @Test
    fun `end input then flush then close writes the batch array of a drain timeout`() =
        runTest {
            for (connect in boundedModesWith(BoundedDispatchPolicy(2, 8, drainTimeout = 2.seconds))) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                val entered = CompletableDeferred<Unit>()
                connection.onRequest("slow", runUntilCancelled(entered))
                connection.onRequest("echo") { params: JsonElement? -> params }
                connection.onNotification("note") { _: JsonElement? -> }
                connection.receive(wireFrame(STUCK_BATCH))
                withTimeout(1.seconds) { entered.await() }
                // The documented shutdown order, with nothing in between.
                connection.endInput()
                connection.flushOutgoing()
                connection.close()
                val reply = assertIs<JsonArray>(wireJson(assertIs<ByteArray>(frames.tryReceive().getOrNull())))
                val byId = reply.associateBy { it.jsonObject.getValue("id") }
                assertEquals(ResponseError.REQUEST_CANCELLED, byId.getValue(JsonPrimitive("slow")).errorCode())
                assertEquals(JsonPrimitive("b"), byId.getValue(JsonPrimitive(2)).jsonObject["result"])
            }
        }

    @Test
    fun `flush after a drain timeout covers the batch array when the collector write suspends`() =
        runTest {
            for (connect in boundedModesWith(BoundedDispatchPolicy(2, 8, drainTimeout = 2.seconds))) {
                val connection = connect(backgroundScope)
                // Like a socket write, each write suspends before it returns.
                val frames = Channel<ByteArray>(Channel.UNLIMITED)
                backgroundScope.launch {
                    connection.outgoing.collect {
                        delay(10.milliseconds)
                        frames.send(it)
                    }
                }
                val entered = CompletableDeferred<Unit>()
                connection.onRequest("slow", runUntilCancelled(entered))
                connection.onRequest("echo") { params: JsonElement? -> params }
                connection.onNotification("note") { _: JsonElement? -> }
                connection.receive(wireFrame(STUCK_BATCH))
                withTimeout(1.seconds) { entered.await() }
                connection.endInput()
                // The barrier must queue behind the array: if endInput returned before the batch
                // sender handed the array over, the flush completes while the array is still unwritten.
                connection.flushOutgoing()
                val written = frames.tryReceive().getOrNull()
                connection.close()
                assertIs<ByteArray>(written, "the flush returned before the array was written")
                val reply = assertIs<JsonArray>(wireJson(written))
                val byId = reply.associateBy { it.jsonObject.getValue("id") }
                assertEquals(ResponseError.REQUEST_CANCELLED, byId.getValue(JsonPrimitive("slow")).errorCode())
                assertEquals(JsonPrimitive("b"), byId.getValue(JsonPrimitive(2)).jsonObject["result"])
            }
        }

    @Test
    fun `a batch array that cannot be sent after a drain timeout fails the connection`() =
        runTest {
            val connection =
                Connection(
                    BoundedDispatchPolicy(2, 8, drainTimeout = 2.seconds),
                    InboundResourceLimits(maxFrameBodyBytes = 65_536, maxJsonNesting = 32, maxApplicationBodyBytes = 65_536),
                    OutboundResourceLimits(1_024, 65_536, 8, 8, 65_536, 4, 10_000, 32),
                    backgroundScope,
                )
            backgroundScope.outgoingFrames(connection)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("slow", runUntilCancelled(entered))
            connection.onRequest("big") { _: JsonElement? -> JsonPrimitive("x".repeat(2_048)) }
            connection.receive(wireFrame("""[{"jsonrpc":"2.0","method":"slow","id":1},{"jsonrpc":"2.0","method":"big","id":2}]"""))
            withTimeout(1.seconds) { entered.await() }
            connection.endInput()
            // The array exceeds the outbound frame limit; the sender reports it instead of dropping it.
            assertIs<ConnectionState.Failed>(withTimeout(1.seconds) { connection.state.first { it !is ConnectionState.Open } })
        }

    @Test
    fun `a batch that repeats a request id fails the connection`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(2, 8), scope = backgroundScope)
            connection.onRequest("echo") { params: JsonElement? -> params }
            val frame = wireFrame("""[{"jsonrpc":"2.0","method":"echo","id":1},{"jsonrpc":"2.0","method":"echo","id":1}]""")
            assertTrue(runCatching { connection.receive(frame) }.isFailure)
            val failed = assertIs<ConnectionState.Failed>(connection.state.value)
            assertEquals("Duplicate active incoming request ID", failed.cause.message)
        }

    @Test
    fun `a batch with more notifications than free queue slots fails the connection`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            connection.onNotification("note") { _: JsonElement? -> }
            val frame = wireFrame("""[{"jsonrpc":"2.0","method":"note"},{"jsonrpc":"2.0","method":"note"}]""")
            assertTrue(runCatching { connection.receive(frame) }.isFailure)
            val failed = assertIs<ConnectionState.Failed>(connection.state.value)
            assertEquals("Incoming ordered queue is full or closed", failed.cause.message)
        }

    @Test
    fun `changing the control set after the connection is created has no effect`() =
        runTest {
            val control = mutableSetOf("control")
            val connection = Connection(BoundedDispatchPolicy(1, 4, controlNotifications = control), scope = backgroundScope)
            control.clear()
            control.add("ordinary")
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val order = mutableListOf<String>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                null
            }
            connection.onNotification("control") { _: JsonElement? -> order.add("control") }
            connection.onNotification("ordinary") { _: JsonElement? -> order.add("ordinary") }
            backgroundScope.outgoingFrames(connection)
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(NotificationMessage("ordinary")))
            connection.receive(codec.encode(NotificationMessage("control")))
            runCurrent()
            assertEquals(listOf("control"), order)
            release.complete(Unit)
            connection.close()
        }

    @Test
    fun `end of input returns at once when the output side has already failed`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 4), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("stuck", runUntilCancelled(entered))
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "stuck")))
            withTimeout(1.seconds) { entered.await() }
            // Interrupting the dedicated collector fails a bounded connection: no response can be written.
            backgroundScope.async { connection.outgoing.first() }
            connection.notify("nudge", null)
            withTimeout(1.seconds) { connection.state.first { it !is ConnectionState.Open } }
            val start = currentTime
            connection.endInput()
            assertEquals(start, currentTime)
            connection.close()
        }

    @Test
    fun `end of input returns at once on a closed connection and in the default mode`() =
        runTest {
            val bounded = Connection(BoundedDispatchPolicy(1, 4), scope = backgroundScope)
            bounded.close()
            bounded.endInput()
            val inline = Connection(scope = backgroundScope)
            inline.onRequest("ping") { _: JsonElement? -> JsonPrimitive("pong") }
            val frames = backgroundScope.outgoingFrames(inline)
            inline.endInput()
            // The default mode is unchanged: it keeps receiving after endInput.
            inline.receive(codec.encode(RequestMessage(RequestId.of(1), "ping")))
            assertEquals(JsonPrimitive("pong"), assertIs<ResponseMessage>(decode(frames.nextFrame())).result)
            assertEquals(0L, currentTime)
            inline.close()
        }

    @Test
    fun `peers sharing one dispatcher cannot cancel requests of another peer`() =
        runTest {
            val dispatcher = Dispatcher()
            val release = CompletableDeferred<Unit>()
            val entered = Array(2) { CompletableDeferred<Unit>() }
            dispatcher.onRequest("slow") { params: JsonElement? ->
                entered[assertIs<JsonPrimitive>(params).int].complete(Unit)
                release.await()
                params
            }
            val first = dispatcher.newPeer()
            val second = dispatcher.newPeer()
            val id = RequestId.of(1)
            val firstResponse = async { first.dispatch(RequestMessage(id, "slow", JsonPrimitive(0))) }
            val secondResponse = async { second.dispatch(RequestMessage(id, "slow", JsonPrimitive(1))) }
            entered.forEach { it.await() }
            assertEquals(1, first.inbound.count)
            assertEquals(1, second.inbound.count)
            second.dispatch(cancel(id))
            assertEquals(ResponseMessage.error(id, ResponseError.requestCancelled()), secondResponse.await())
            assertTrue(firstResponse.isActive)
            assertEquals(1, first.inbound.count)
            release.complete(Unit)
            assertEquals(ResponseMessage.success(id, JsonPrimitive(0)), firstResponse.await())
        }

    @Test
    fun `peers sharing one dispatcher complete only their own outbound waiters`() =
        runTest {
            val dispatcher = Dispatcher()
            val first = dispatcher.newPeer()
            val second = dispatcher.newPeer()
            val id = RequestId.of(9)
            val firstWaiter = first.registerPendingRequest(id)
            val secondWaiter = second.registerPendingRequest(id)
            second.dispatch(ResponseMessage.success(id, JsonPrimitive("second")))
            assertEquals(JsonPrimitive("second"), secondWaiter.await())
            assertTrue(firstWaiter.isActive)
            first.dispatch(ResponseMessage.success(id, JsonPrimitive("first")))
            assertEquals(JsonPrimitive("first"), firstWaiter.await())
        }

    @Test
    fun `cancelling one entry of a mixed batch answers it as cancelled in the single array`() =
        runTest {
            for (connect in boundedModes) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                val entered = CompletableDeferred<Unit>()
                val observed = CompletableDeferred<Unit>()
                val notes = mutableListOf<JsonElement?>()
                connection.onRequest("slow", runUntilCancelled(entered, observed))
                connection.onRequest("echo") { params: JsonElement? -> params }
                connection.onNotification("note") { params: JsonElement? -> notes.add(params) }
                connection.receive(wireFrame(MIXED_BATCH))
                withTimeout(1.seconds) { entered.await() }
                runCurrent()
                // The notification entry is a barrier behind the running slow entry, so nothing is sent yet.
                assertTrue(frames.isEmpty)
                assertTrue(notes.isEmpty())
                connection.receive(codec.encode(cancel(RequestId.of("slow"))))
                withTimeout(1.seconds) { observed.await() }
                val reply = assertIs<JsonArray>(wireJson(frames.nextFrame()))
                val byId = reply.associateBy { it.jsonObject.getValue("id") }
                assertEquals(setOf(JsonPrimitive("slow"), JsonPrimitive(2), JsonNull), byId.keys)
                assertEquals(3, reply.size)
                assertEquals(ResponseError.REQUEST_CANCELLED, byId.getValue(JsonPrimitive("slow")).errorCode())
                assertEquals(JsonPrimitive("b"), byId.getValue(JsonPrimitive(2)).jsonObject["result"])
                assertEquals(ResponseError.INVALID_REQUEST, byId.getValue(JsonNull).errorCode())
                assertEquals(listOf<JsonElement?>(JsonPrimitive("n")), notes)
                runCurrent()
                assertTrue(frames.isEmpty, "a batch is answered with exactly one frame")
                assertEquals(ConnectionState.Open, connection.state.value)
                connection.close()
            }
        }

    @Test
    fun `batch requests run concurrently and keep their ids active until the array is dequeued`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(2, 4), scope = backgroundScope)
            val entered = Array(2) { CompletableDeferred<Unit>() }
            connection.onRequest("both") { params: JsonElement? ->
                val index = assertIs<JsonPrimitive>(params).int
                entered[index].complete(Unit)
                // Each entry waits for the other, so this finishes only if they run at the same time.
                entered[1 - index].await()
                params
            }
            connection.receive(
                wireFrame("""[{"jsonrpc":"2.0","method":"both","params":0,"id":1},{"jsonrpc":"2.0","method":"both","params":1,"id":2}]"""),
            )
            withTimeout(1.seconds) { entered.forEach { it.await() } }
            runCurrent()
            // No collector yet, so the array is not dequeued and a reused id is a duplicate.
            assertTrue(
                runCatching { connection.receive(codec.encode(RequestMessage(RequestId.of(1), "both", JsonPrimitive(0)))) }.isFailure,
            )
            val failed = assertIs<ConnectionState.Failed>(connection.state.value)
            assertEquals("Duplicate active incoming request ID", failed.cause.message)
        }

    @Test
    fun `a running handler observes a control notification`() =
        runTest {
            val progressCancel = "window/workDoneProgress/cancel"
            val policy = BoundedDispatchPolicy(1, 4, controlNotifications = setOf(progressCancel, "run/readStdin"))
            val connection = Connection(policy, scope = backgroundScope)
            val frames = backgroundScope.outgoingFrames(connection)
            val entered = CompletableDeferred<Unit>()
            val stop = CompletableDeferred<Unit>()
            val input = mutableListOf<JsonElement?>()
            connection.onRequest("long") { _: JsonElement? ->
                entered.complete(Unit)
                stop.await()
                JsonArray(input.toList().map { it ?: JsonNull })
            }
            connection.onNotification("run/readStdin") { params: JsonElement? -> input.add(params) }
            connection.onNotification(progressCancel) { _: JsonElement? -> stop.complete(Unit) }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "long")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(NotificationMessage("run/readStdin", JsonPrimitive("a"))))
            connection.receive(codec.encode(NotificationMessage("run/readStdin", JsonPrimitive("b"))))
            connection.receive(codec.encode(NotificationMessage(progressCancel, JsonObject(mapOf("token" to JsonPrimitive("t"))))))
            val response = assertIs<ResponseMessage>(decode(frames.nextFrame()))
            // The handler saw both inputs, in arrival order, while it was still running.
            assertEquals(JsonArray(listOf(JsonPrimitive("a"), JsonPrimitive("b"))), response.result)
            connection.close()
        }

    @Test
    fun `notifications outside the control list remain barriers`() =
        runTest {
            val policy = BoundedDispatchPolicy(1, 4, controlNotifications = setOf("control"))
            val connection = Connection(policy, scope = backgroundScope)
            val frames = backgroundScope.outgoingFrames(connection)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val order = mutableListOf<String>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                order.add("request")
                null
            }
            connection.onNotification("control") { _: JsonElement? -> order.add("control") }
            connection.onNotification("ordinary") { _: JsonElement? -> order.add("ordinary") }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(NotificationMessage("ordinary")))
            connection.receive(codec.encode(NotificationMessage("control")))
            runCurrent()
            assertEquals(listOf("control"), order)
            release.complete(Unit)
            frames.nextFrame()
            runCurrent()
            assertEquals(listOf("control", "request", "ordinary"), order)
            connection.close()
        }

    /** A handler that runs until it is cancelled and records that it observed the cancellation. */
    private fun runUntilCancelled(
        entered: CompletableDeferred<Unit>,
        observed: CompletableDeferred<Unit> = CompletableDeferred(),
    ): RequestHandler =
        RequestHandler {
            entered.complete(Unit)
            try {
                awaitCancellation()
            } catch (cause: CancellationException) {
                observed.complete(Unit)
                throw cause
            }
        }

    /** The bounded dispatch modes with [policy]: without limits, and with inbound and outbound resource limits. */
    private fun boundedModesWith(policy: BoundedDispatchPolicy): List<(CoroutineScope) -> Connection> =
        listOf(
            { scope -> Connection(policy, scope = scope) },
            { scope ->
                Connection(
                    policy,
                    InboundResourceLimits(maxFrameBodyBytes = 65_536, maxJsonNesting = 32, maxApplicationBodyBytes = 65_536),
                    OutboundResourceLimits(65_536, 65_536, 8, 8, 65_536, 4, 10_000, 32),
                    scope,
                )
            },
        )

    private fun decode(frame: ByteArray): Message = JsonRpcMessageDecoder(codec).feed(frame).messages().single()

    private fun cancel(id: RequestId): NotificationMessage = NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD, cancelRequestParams(id))

    private fun JsonElement.errorCode(): Int =
        jsonObject
            .getValue("error")
            .jsonObject
            .getValue("code")
            .jsonPrimitive.int

    private companion object {
        /** A cancellable request, a notification, a quick request and an invalid entry. */
        const val MIXED_BATCH =
            """[{"jsonrpc":"2.0","method":"slow","id":"slow"},{"jsonrpc":"2.0","method":"note","params":"n"},""" +
                """{"jsonrpc":"2.0","method":"echo","params":"b","id":2},7]"""

        /** A quick request, a notification, a request that never finishes, another quick request and an invalid entry. */
        const val STUCK_BATCH =
            """[{"jsonrpc":"2.0","method":"echo","params":"a","id":1},{"jsonrpc":"2.0","method":"note","params":"n"},""" +
                """{"jsonrpc":"2.0","method":"slow","id":"slow"},{"jsonrpc":"2.0","method":"echo","params":"b","id":2},7]"""

        /** The bounded dispatch modes: without limits, and with inbound and outbound resource limits. */
        val boundedModes = dispatchModes.drop(1)
    }
}
