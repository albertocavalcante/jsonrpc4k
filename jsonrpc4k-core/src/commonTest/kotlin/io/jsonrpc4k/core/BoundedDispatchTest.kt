package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class BoundedDispatchTest {
    private val codec = JsonRpcCodec.Default
    private val policy = BoundedDispatchPolicy(maxConcurrentRequests = 1, maxQueuedMessages = 2)

    @Test
    fun `policy rejects unbounded or nonpositive limits`() {
        assertFailsWith<IllegalArgumentException> { BoundedDispatchPolicy(0, 1) }
        assertFailsWith<IllegalArgumentException> { BoundedDispatchPolicy(65, 1) }
        assertFailsWith<IllegalArgumentException> { BoundedDispatchPolicy(1, 0) }
        assertFailsWith<IllegalArgumentException> { BoundedDispatchPolicy(1, 1025) }
    }

    @Test
    fun `close interrupts admission of a large coalesced response batch`() =
        runTest {
            val parent = SupervisorJob(backgroundScope.coroutineContext[Job])
            val connection = Connection(policy, scope = CoroutineScope(backgroundScope.coroutineContext + parent))
            val frame = codec.encode(ResponseMessage.success(RequestId.of(42), JsonPrimitive("ok")))
            val batch = ByteArray(frame.size * 1000) { frame[it % frame.size] }
            val receive = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.receive(batch) }
            assertFalse(receive.isCompleted, "admission must give cancellation a scheduling opportunity")
            connection.close()
            assertFailsWith<kotlinx.coroutines.CancellationException> { withTimeout(1.seconds) { receive.await() } }
            assertEquals(ConnectionState.Closed, connection.state.value)
            assertTrue(parent.isActive)
            parent.cancel()
        }

    @Test
    fun `close cancels owned handlers without cancelling parent or sibling`() =
        runTest {
            val parent = SupervisorJob(backgroundScope.coroutineContext[Job])
            val sibling = Job(parent)
            val connection = Connection(policy, scope = CoroutineScope(backgroundScope.coroutineContext + parent))
            val entered = CompletableDeferred<Unit>()
            val stopped = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                try {
                    entered.complete(Unit)
                    CompletableDeferred<Unit>().await()
                    null
                } finally {
                    stopped.complete(Unit)
                }
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.close()
            withTimeout(1.seconds) { stopped.await() }
            assertEquals(ConnectionState.Closed, connection.state.value)
            assertTrue(parent.isActive)
            assertTrue(sibling.isActive)
            parent.cancel()
        }

    @Test
    fun `interrupted writer closes bounded connection and pending waiter without touching parent`() =
        runTest {
            val parent = SupervisorJob(backgroundScope.coroutineContext[Job])
            val sibling = Job(parent)
            val connection = Connection(policy, scope = CoroutineScope(backgroundScope.coroutineContext + parent))
            val writer = backgroundScope.launch { connection.outgoing.collect {} }
            val outbound = backgroundScope.async { runCatching { connection.request<JsonElement>("outbound", null) } }
            runCurrent()
            assertFalse(outbound.isCompleted)
            writer.cancel()
            withTimeout(1.seconds) { writer.join() }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(withTimeout(1.seconds) { outbound.await() }.isFailure)
            assertTrue(parent.isActive)
            assertTrue(sibling.isActive)
            parent.cancel()
            assertEquals(terminal, connection.state.value)
        }

    @Test
    fun `writer canceled after receiving a frame cannot strand its waiter`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val writer = backgroundScope.launch { connection.outgoing.collect {} }
            runCurrent()
            val outbound =
                backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
                    runCatching { connection.request<JsonElement>("outbound", null) }
                }
            writer.cancel()
            withTimeout(1.seconds) { writer.join() }
            assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(withTimeout(1.seconds) { outbound.await() }.isFailure)
        }

    @Test
    fun `rejected second writer does not terminate the active bounded writer`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val writer = backgroundScope.launch { connection.outgoing.collect {} }
            runCurrent()
            withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.outgoing.first() } }
            assertEquals(ConnectionState.Open, connection.state.value)
            connection.close()
            withTimeout(1.seconds) { writer.join() }
            assertEquals(ConnectionState.Closed, connection.state.value)
        }

    @Test
    fun `parent cancellation is not replaced by writer failure`() =
        runTest {
            val parent = SupervisorJob(backgroundScope.coroutineContext[Job])
            val scope = CoroutineScope(backgroundScope.coroutineContext + parent)
            val connection = Connection(policy, scope = scope)
            val writer = scope.launch { connection.outgoing.collect {} }
            runCurrent()
            parent.cancel()
            withTimeout(1.seconds) { writer.join() }
            assertEquals(ConnectionState.Closed, connection.state.value)
        }

    @Test
    fun `first terminates opt in connection but leaves legacy connection open`() =
        runTest {
            val legacy = Connection(scope = backgroundScope)
            val legacyFrame = backgroundScope.async { legacy.outgoing.first() }
            legacy.notify("legacy", null)
            withTimeout(1.seconds) { legacyFrame.await() }
            assertEquals(ConnectionState.Open, legacy.state.value)
            legacy.close()

            val bounded = Connection(policy, scope = backgroundScope)
            val boundedFrame = backgroundScope.async { bounded.outgoing.first() }
            bounded.notify("bounded", null)
            withTimeout(1.seconds) { boundedFrame.await() }
            assertIs<ConnectionState.Failed>(bounded.state.value)
        }

    @Test
    fun `malformed frame is terminal and retains the framing failure`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val failure =
                assertFailsWith<JsonRpcProtocolException> {
                    connection.receive("Content-Length: -1\r\n\r\n")
                }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(generateSequence<Throwable>(failure) { it.cause }.take(8).any { it === terminal.cause })
            connection.close()
            assertEquals(terminal, connection.state.value)
        }

    @Test
    fun `fatal notification failure terminates instead of stranding waiters`() =
        runTest {
            val parent = SupervisorJob(backgroundScope.coroutineContext[Job])
            val reported = CompletableDeferred<Throwable>()
            val handler = CoroutineExceptionHandler { _, failure -> reported.complete(failure) }
            val connection = Connection(policy, scope = CoroutineScope(backgroundScope.coroutineContext + parent + handler))
            val fatal = AssertionError("fatal handler")
            connection.onNotification("explode") { _: JsonElement? -> throw fatal }
            val outbound = backgroundScope.async { runCatching { connection.request<JsonElement>("outbound", null) } }
            runCurrent()
            assertFalse(outbound.isCompleted)
            connection.receive(codec.encode(NotificationMessage("explode")))
            val uncaught = withTimeout(1.seconds) { reported.await() }
            assertTrue(generateSequence<Throwable>(uncaught) { it.cause }.take(8).any { it === fatal })
            assertTrue(withTimeout(1.seconds) { outbound.await() }.isFailure)
            assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(parent.isActive)
            parent.cancel()
        }

    @Test
    fun `default receive remains synchronous`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                null
            }
            val received = backgroundScope.async { connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait"))) }
            entered.await()
            assertFalse(received.isCompleted)
            release.complete(Unit)
            withTimeout(1.seconds) { received.await() }
            connection.close()
        }

    @Test
    fun `notifications wait for earlier requests and block later requests`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val order = mutableListOf<String>()
            connection.onRequest("first") { _: JsonElement? ->
                order.add("first start")
                entered.complete(Unit)
                release.await()
                order.add("first end")
                null
            }
            connection.onNotification("changed") { _: JsonElement? -> order.add("changed") }
            connection.onRequest("second") { _: JsonElement? ->
                order.add("second")
                null
            }
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "first")))
            entered.await()
            connection.receive(codec.encode(NotificationMessage("changed")))
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "second")))
            runCurrent()
            assertEquals(listOf("first start"), order)
            release.complete(Unit)
            withTimeout(1.seconds) { repeat(2) { frames.receive() } }
            assertEquals(listOf("first start", "first end", "changed", "second"), order)
            connection.close()
        }

    @Test
    fun `incoming cancellation never cancels colliding outbound waiter`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("incoming") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            val outbound = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.request<JsonElement>("outbound", null) }
            val sentRequest = decode(withTimeout(1.seconds) { frames.receive() })
            assertIs<RequestMessage>(sentRequest)
            assertEquals(RequestId.of(1), sentRequest.id)
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "incoming")))
            entered.await()
            connection.receive(codec.encode(cancel(RequestId.of(1))))
            val cancelled = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(ResponseError.REQUEST_CANCELLED, cancelled.error?.code)
            assertFalse(outbound.isCompleted)
            connection.receive(codec.encode(ResponseMessage.success(RequestId.of(1), JsonPrimitive("ok"))))
            assertEquals(JsonPrimitive("ok"), withTimeout(1.seconds) { outbound.await() })
            connection.close()
        }

    @Test
    fun `queued cancellation skips the handler and returns one cancellation response`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            var secondStarted = false
            connection.onRequest("first") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                null
            }
            connection.onRequest("second") { _: JsonElement? ->
                secondStarted = true
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "first")))
            entered.await()
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "second")))
            connection.receive(codec.encode(cancel(RequestId.of(2))))
            val cancelled = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(RequestId.of(2), cancelled.id)
            assertEquals(ResponseError.REQUEST_CANCELLED, cancelled.error?.code)
            assertFalse(secondStarted)
            release.complete(Unit)
            val first = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(RequestId.of(1), first.id)
            runCurrent()
            assertTrue(frames.isEmpty)
            connection.close()
        }

    @Test
    fun `late cancellation cannot duplicate a response blocked by a full output`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val filled = CompletableDeferred<Unit>()
            connection.onRequest("burst") { _: JsonElement? ->
                repeat(64) { connection.notify("update", null) }
                filled.complete(Unit)
                JsonPrimitive("done")
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(7), "burst")))
            withTimeout(1.seconds) { filled.await() }
            runCurrent()
            connection.receive(codec.encode(cancel(RequestId.of(7))))
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val messages = withTimeout(1.seconds) { List(65) { decode(frames.receive()) } }
            val response = assertIs<ResponseMessage>(messages.last())
            assertEquals(RequestId.of(7), response.id)
            assertEquals(JsonPrimitive("done"), response.result)
            runCurrent()
            assertTrue(frames.isEmpty)
            assertEquals(ConnectionState.Open, connection.state.value)
            connection.close()
        }

    @Test
    fun `cancellation response is sent before following notification barrier`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val entered = CompletableDeferred<Unit>()
            val notificationDone = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.onNotification("barrier") { _: JsonElement? ->
                connection.flushOutgoing()
                val response = assertIs<ResponseMessage>(decode(frames.tryReceive().getOrThrow()))
                assertEquals(ResponseError.REQUEST_CANCELLED, response.error?.code)
                notificationDone.complete(Unit)
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(5), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(cancel(RequestId.of(5))))
            connection.receive(codec.encode(NotificationMessage("barrier")))
            withTimeout(1.seconds) { notificationDone.await() }
            connection.close()
        }

    @Test
    fun `cancelled ID may be reused while the old handler is still cleaning up`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(2, 4), scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val entered = CompletableDeferred<Unit>()
            val cleaning = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            connection.onRequest("old") { _: JsonElement? ->
                try {
                    entered.complete(Unit)
                    CompletableDeferred<Unit>().await()
                    null
                } finally {
                    withContext(NonCancellable) {
                        cleaning.complete(Unit)
                        release.await()
                    }
                }
            }
            connection.onRequest("new") { _: JsonElement? -> JsonPrimitive("new") }
            val id = RequestId.of("same")
            connection.receive(codec.encode(RequestMessage(id, "old")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(cancel(id)))
            val cancelled = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(ResponseError.REQUEST_CANCELLED, cancelled.error?.code)
            withTimeout(1.seconds) { cleaning.await() }
            connection.receive(codec.encode(RequestMessage(id, "new")))
            val result = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(JsonPrimitive("new"), result.result)
            assertEquals(ConnectionState.Open, connection.state.value)
            release.complete(Unit)
            connection.close()
        }

    @Test
    fun `notification barrier cannot overtake cancellation response offer`() =
        runTest {
            val job = SupervisorJob(backgroundScope.coroutineContext[Job])
            val scope = CoroutineScope(backgroundScope.coroutineContext + job)
            val dispatcher = Dispatcher()
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val notification = CompletableDeferred<Unit>()
            val offered = mutableListOf<ResponseMessage>()
            dispatcher.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                null
            }
            dispatcher.onNotification("barrier") { _: JsonElement? -> notification.complete(Unit) }
            val drive = { runCurrent() }
            val output =
                object : DispatchOutput {
                    override suspend fun send(
                        response: ResponseMessage,
                        onDequeued: () -> Unit,
                    ) {
                        onDequeued()
                        offered.add(response)
                    }

                    override fun offer(
                        response: ResponseMessage,
                        onDequeued: (() -> Unit)?,
                    ): Boolean {
                        release.complete(Unit)
                        drive()
                        assertFalse(notification.isCompleted)
                        offered.add(response)
                        onDequeued?.invoke()
                        return true
                    }

                    override fun fail(cause: Throwable): Unit = throw cause
                }
            val pump = BoundedDispatchPump(policy, JsonRpcMessageDecoder(codec), dispatcher, scope, output)
            pump.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            pump.receive(codec.encode(NotificationMessage("barrier")))
            runCurrent()
            pump.receive(codec.encode(cancel(RequestId.of(1))))
            withTimeout(1.seconds) { notification.await() }
            assertEquals(listOf(ResponseError.REQUEST_CANCELLED), offered.map { it.error?.code })
            job.cancel()
        }

    @Test
    fun `response ID can be reused when writer sees it before sender resumes`() =
        runTest {
            val job = SupervisorJob(backgroundScope.coroutineContext[Job])
            val scope = CoroutineScope(backgroundScope.coroutineContext + job)
            val dispatcher = Dispatcher()
            val secondSent = CompletableDeferred<Unit>()
            val releaseFirst = CompletableDeferred<Unit>()
            var terminal: Throwable? = null
            lateinit var pump: BoundedDispatchPump
            dispatcher.onRequest("first") { _: JsonElement? -> JsonPrimitive("first") }
            dispatcher.onRequest("second") { _: JsonElement? -> JsonPrimitive("second") }
            val output =
                object : DispatchOutput {
                    override suspend fun send(
                        response: ResponseMessage,
                        onDequeued: () -> Unit,
                    ) {
                        onDequeued()
                        if (response.result == JsonPrimitive("first")) {
                            pump.receive(codec.encode(RequestMessage(RequestId.of(1), "second")))
                            releaseFirst.await()
                        } else {
                            secondSent.complete(Unit)
                        }
                    }

                    override fun offer(
                        response: ResponseMessage,
                        onDequeued: (() -> Unit)?,
                    ): Boolean = error("Unexpected control response: $response")

                    override fun fail(cause: Throwable) {
                        terminal = cause
                    }
                }
            pump = BoundedDispatchPump(BoundedDispatchPolicy(2, 4), JsonRpcMessageDecoder(codec), dispatcher, scope, output)
            pump.receive(codec.encode(RequestMessage(RequestId.of(1), "first")))
            withTimeout(1.seconds) { secondSent.await() }
            assertEquals(null, terminal)
            releaseFirst.complete(Unit)
            job.cancel()
        }

    @Test
    fun `parent cancellation clears waiters before noncancellable handler cleanup finishes`() =
        runTest {
            val parent = SupervisorJob(backgroundScope.coroutineContext[Job])
            val connection = Connection(policy, scope = CoroutineScope(backgroundScope.coroutineContext + parent))
            val entered = CompletableDeferred<Unit>()
            val cleaning = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                try {
                    entered.complete(Unit)
                    CompletableDeferred<Unit>().await()
                    null
                } finally {
                    withContext(NonCancellable) {
                        cleaning.complete(Unit)
                        release.await()
                    }
                }
            }
            val outbound = backgroundScope.async { runCatching { connection.request<JsonElement>("outbound", null) } }
            connection.receive(codec.encode(RequestMessage(RequestId.of(5), "wait")))
            withTimeout(1.seconds) { entered.await() }
            runCurrent()
            assertFalse(outbound.isCompleted)
            parent.cancel()
            withTimeout(1.seconds) { cleaning.await() }
            assertEquals(ConnectionState.Closed, connection.state.value)
            assertTrue(withTimeout(1.seconds) { outbound.await() }.isFailure)
            release.complete(Unit)
        }

    @Test
    fun `duplicate active inbound ID terminates with observable cause`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of("same"), "wait")))
            entered.await()
            assertFailsWith<JsonRpcProtocolException> {
                connection.receive(codec.encode(RequestMessage(RequestId.of("same"), "wait")))
            }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(
                terminal.cause.message
                    .orEmpty()
                    .contains("Duplicate"),
            )
            connection.close()
            assertEquals(terminal, connection.state.value)
        }

    @Test
    fun `buffered response keeps its ID active until dequeued`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val handled = CompletableDeferred<Unit>()
            connection.onRequest("done") { _: JsonElement? ->
                handled.complete(Unit)
                JsonPrimitive("done")
            }
            val id = RequestId.of(9)
            connection.receive(codec.encode(RequestMessage(id, "done")))
            withTimeout(1.seconds) { handled.await() }
            runCurrent()
            assertFailsWith<JsonRpcProtocolException> { connection.receive(codec.encode(RequestMessage(id, "done"))) }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(
                terminal.cause.message
                    .orEmpty()
                    .contains("Duplicate"),
            )
        }

    @Test
    fun `response ID can be reused after the frame is dequeued`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            connection.onRequest("done") { _: JsonElement? -> JsonPrimitive("done") }
            val id = RequestId.of(9)
            repeat(2) {
                connection.receive(codec.encode(RequestMessage(id, "done")))
                val response = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
                assertEquals(id, response.id)
                assertEquals(JsonPrimitive("done"), response.result)
            }
            assertEquals(ConnectionState.Open, connection.state.value)
            connection.close()
        }

    @Test
    fun `full queue rejects a request without blocking responses`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            connection.onRequest("first") { _: JsonElement? ->
                entered.complete(Unit)
                release.await()
                null
            }
            connection.onRequest("second") { _: JsonElement? -> null }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "first")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "second")))
            runCurrent()
            connection.receive(codec.encode(RequestMessage(RequestId.of(3), "second")))
            val busy = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(RequestId.of(3), busy.id)
            assertEquals(-32000, busy.error?.code)
            assertEquals(ConnectionState.Open, connection.state.value)
            release.complete(Unit)
            withTimeout(1.seconds) { repeat(2) { frames.receive() } }
            connection.close()
        }

    @Test
    fun `buffered busy response keeps its ID active until dequeued`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "wait")))
            runCurrent()
            connection.receive(codec.encode(RequestMessage(RequestId.of(3), "wait")))
            assertFailsWith<JsonRpcProtocolException> { connection.receive(codec.encode(RequestMessage(RequestId.of(3), "wait"))) }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(
                terminal.cause.message
                    .orEmpty()
                    .contains("Duplicate"),
            )
        }

    @Test
    fun `cancellation cannot add a second response to buffered busy rejection`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "wait")))
            runCurrent()
            connection.receive(codec.encode(RequestMessage(RequestId.of(3), "wait")))
            connection.receive(codec.encode(cancel(RequestId.of(3))))
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val busy = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(RequestId.of(3), busy.id)
            assertEquals(-32000, busy.error?.code)
            runCurrent()
            assertTrue(frames.isEmpty)
            connection.close()
        }

    @Test
    fun `responses bypass a saturated application queue`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            val outbound = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.request<JsonElement>("outbound", null) }
            connection.receive(codec.encode(RequestMessage(RequestId.of(10), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(RequestMessage(RequestId.of(11), "wait")))
            runCurrent()
            connection.receive(codec.encode(ResponseMessage.success(RequestId.of(1), JsonPrimitive("ok"))))
            assertEquals(JsonPrimitive("ok"), withTimeout(1.seconds) { outbound.await() })
            connection.close()
        }

    @Test
    fun `malformed cancellation is ignored while a valid string cancellation works`() =
        runTest {
            val connection = Connection(policy, scope = backgroundScope)
            val frames = Channel<ByteArray>(Channel.UNLIMITED)
            backgroundScope.launch { connection.outgoing.collect { frames.send(it) } }
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of("same"), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(
                codec.encode(NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD, buildJsonObject { put("id", JsonPrimitive(true)) })),
            )
            connection.receive(codec.encode(NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD)))
            runCurrent()
            assertTrue(frames.isEmpty)
            connection.receive(codec.encode(cancel(RequestId.of("same"))))
            val cancelled = assertIs<ResponseMessage>(decode(withTimeout(1.seconds) { frames.receive() }))
            assertEquals(RequestId.of("same"), cancelled.id)
            assertEquals(ResponseError.REQUEST_CANCELLED, cancelled.error?.code)
            connection.close()
        }

    @Test
    fun `notification overflow terminates without cancelling parent`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "wait")))
            runCurrent()
            assertFailsWith<JsonRpcProtocolException> { connection.receive(codec.encode(NotificationMessage("overflow"))) }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(
                terminal.cause.message
                    .orEmpty()
                    .contains("queue is full"),
            )
            assertTrue(backgroundScope.coroutineContext[kotlinx.coroutines.Job]!!.isActive)
        }

    @Test
    fun `busy response overflow terminates instead of blocking the input pump`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(1, 1), scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            repeat(64) { connection.notify("fill", null) }
            connection.onRequest("wait") { _: JsonElement? ->
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
                null
            }
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "wait")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(RequestMessage(RequestId.of(2), "wait")))
            runCurrent()
            assertFailsWith<JsonRpcProtocolException> { connection.receive(codec.encode(RequestMessage(RequestId.of(3), "wait"))) }
            val terminal = assertIs<ConnectionState.Failed>(connection.state.value)
            assertTrue(
                terminal.cause.message
                    .orEmpty()
                    .contains("Outgoing response queue is full"),
            )
        }

    private fun decode(frame: ByteArray): Message = JsonRpcMessageDecoder(codec).feed(frame).single()

    private fun cancel(id: RequestId): NotificationMessage {
        val wireId =
            when (id) {
                is RequestId.NumberId -> JsonPrimitive(id.value)
                is RequestId.StringId -> JsonPrimitive(id.value)
            }
        return NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD, buildJsonObject { put("id", wireId) })
    }
}
