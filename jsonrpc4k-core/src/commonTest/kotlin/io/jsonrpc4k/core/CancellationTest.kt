package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** `$/cancelRequest` (LSP 3.18 base protocol) in both directions, in the dispatcher and the connection. */
@OptIn(ExperimentalCoroutinesApi::class)
class CancellationTest {
    private val codec = JsonRpcCodec.Default

    @Test
    fun `cancel request stops a running handler and answers request cancelled`() =
        runTest {
            val dispatcher = Dispatcher()
            val entered = CompletableDeferred<Unit>()
            val observed = CompletableDeferred<Unit>()
            dispatcher.onRequest("slow", runUntilCancelled(entered, observed))
            val response = async { dispatcher.dispatch(RequestMessage(RequestId.of(1), "slow")) }
            entered.await()
            assertEquals(1, dispatcher.inbound.count)
            assertNull(dispatcher.dispatch(cancel(RequestId.of(1))))
            assertEquals(ResponseMessage.error(RequestId.of(1), ResponseError.requestCancelled()), response.await())
            assertTrue(observed.isCompleted)
            assertEquals(0, dispatcher.inbound.count)
        }

    @Test
    fun `cancel request for an unknown or malformed id leaves the handler running`() =
        runTest {
            val dispatcher = Dispatcher()
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            dispatcher.onRequest(
                "wait",
                RequestHandler {
                    entered.complete(Unit)
                    release.await()
                    JsonPrimitive("done")
                },
            )
            val response = async { dispatcher.dispatch(RequestMessage(RequestId.of(1), "wait")) }
            entered.await()
            val ignored =
                listOf(
                    cancel(RequestId.of(2)),
                    cancel(RequestId.of("1")),
                    NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD),
                    NotificationMessage(Dispatcher.CANCEL_REQUEST_METHOD, JsonObject(mapOf("id" to JsonPrimitive(true)))),
                )
            ignored.forEach { assertNull(dispatcher.dispatch(it)) }
            assertEquals(1, dispatcher.inbound.count)
            release.complete(Unit)
            assertEquals(ResponseMessage.success(RequestId.of(1), JsonPrimitive("done")), response.await())
        }

    @Test
    fun `inbound cancel never completes an outbound waiter with the same id`() =
        runTest {
            val dispatcher = Dispatcher()
            val id = RequestId.of(7)
            val waiter = dispatcher.registerPendingRequest(id)
            val entered = CompletableDeferred<Unit>()
            dispatcher.onRequest("slow", runUntilCancelled(entered))
            val incoming = async { dispatcher.dispatch(RequestMessage(id, "slow")) }
            entered.await()
            dispatcher.dispatch(cancel(id))
            assertEquals(ResponseError.REQUEST_CANCELLED, incoming.await()?.error?.code)
            assertTrue(waiter.isActive)
            dispatcher.dispatch(ResponseMessage.success(id, JsonPrimitive("outbound")))
            assertEquals(JsonPrimitive("outbound"), waiter.await())
        }

    @Test
    fun `cancelled handler may answer with its own error`() =
        runTest {
            val dispatcher = Dispatcher()
            val entered = CompletableDeferred<Unit>()
            val partial = JsonPrimitive("partial")
            dispatcher.onRequest(
                "partial",
                RequestHandler {
                    entered.complete(Unit)
                    try {
                        awaitCancellation()
                    } catch (_: CancellationException) {
                        throw JsonRpcException(ResponseError.REQUEST_CANCELLED, "Stopped early", partial)
                    }
                },
            )
            val response = async { dispatcher.dispatch(RequestMessage(RequestId.of(3), "partial")) }
            entered.await()
            dispatcher.dispatch(cancel(RequestId.of(3)))
            assertEquals(ResponseError(ResponseError.REQUEST_CANCELLED, "Stopped early", partial), response.await()?.error)
        }

    @Test
    fun `cancelled id may be reused while the old handler is still finishing`() =
        runTest {
            val dispatcher = Dispatcher()
            val oldEntered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            dispatcher.onRequest(
                "old",
                RequestHandler {
                    oldEntered.complete(Unit)
                    try {
                        awaitCancellation()
                    } finally {
                        withContext(NonCancellable) { release.await() }
                    }
                },
            )
            val newEntered = CompletableDeferred<Unit>()
            dispatcher.onRequest("new", runUntilCancelled(newEntered))
            val id = RequestId.of("same")
            val old = async { dispatcher.dispatch(RequestMessage(id, "old")) }
            oldEntered.await()
            dispatcher.dispatch(cancel(id))
            val new = async { dispatcher.dispatch(RequestMessage(id, "new")) }
            newEntered.await()
            release.complete(Unit)
            assertEquals(ResponseError.REQUEST_CANCELLED, old.await()?.error?.code)
            // The old handler finished last, but the reused id still names the new one.
            assertEquals(1, dispatcher.inbound.count)
            dispatcher.dispatch(cancel(id))
            assertEquals(ResponseError.REQUEST_CANCELLED, new.await()?.error?.code)
            assertEquals(0, dispatcher.inbound.count)
        }

    @Test
    fun `bounded connection cancels a running handler and answers request cancelled`() =
        runTest {
            val connection = Connection(BoundedDispatchPolicy(maxConcurrentRequests = 1, maxQueuedMessages = 2), scope = backgroundScope)
            val frames = backgroundScope.outgoingFrames(connection)
            val entered = CompletableDeferred<Unit>()
            val observed = CompletableDeferred<Unit>()
            connection.onRequest("slow", runUntilCancelled(entered, observed))
            connection.receive(codec.encode(RequestMessage(RequestId.of(1), "slow")))
            withTimeout(1.seconds) { entered.await() }
            connection.receive(codec.encode(cancel(RequestId.of(1))))
            assertEquals(ResponseMessage.error(RequestId.of(1), ResponseError.requestCancelled()), decode(frames.nextFrame()))
            withTimeout(1.seconds) { observed.await() }
            assertEquals(ConnectionState.Open, connection.state.value)
            connection.close()
        }

    @Test
    fun `cancel request for an unknown id is ignored in every dispatch mode`() =
        runTest {
            for (connect in dispatchModes) {
                val connection = connect(backgroundScope)
                connection.onRequest("ping", RequestHandler { JsonPrimitive("pong") })
                val frames = backgroundScope.outgoingFrames(connection)
                connection.receive(codec.encode(cancel(RequestId.of(99))))
                connection.receive(codec.encode(cancel(RequestId.of("missing"))))
                connection.receive(codec.encode(RequestMessage(RequestId.of(1), "ping")))
                // The ping reply is the next frame, so the cancellations sent nothing.
                assertEquals(ResponseMessage.success(RequestId.of(1), JsonPrimitive("pong")), decode(frames.nextFrame()))
                assertEquals(ConnectionState.Open, connection.state.value)
                connection.close()
            }
        }

    @Test
    fun `cancelling a request caller sends cancel request and stops waiting`() =
        runTest {
            for (connect in dispatchModes) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                val call = backgroundScope.async { connection.request<JsonElement>("slow", null) }
                assertEquals(RequestId.of(1), assertIs<RequestMessage>(decode(frames.nextFrame())).id)
                call.cancel()
                assertEquals(cancel(RequestId.of(1)), decode(frames.nextFrame()))
                assertFailsWith<CancellationException> { call.await() }
                assertEquals(0, connection.dispatcher.pending.count)
                // A late response finds no waiter and is ignored.
                connection.receive(codec.encode(ResponseMessage.success(RequestId.of(1), JsonPrimitive("late"))))
                assertEquals(ConnectionState.Open, connection.state.value)
                connection.close()
            }
        }

    @Test
    fun `request timeout sends cancel request`() =
        runTest {
            for (connect in dispatchModes) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                val error = assertFailsWith<JsonRpcException> { connection.request<JsonElement>("slow", null, 10.milliseconds) }
                assertEquals(ResponseError.REQUEST_CANCELLED, error.code)
                assertEquals(RequestId.of(1), assertIs<RequestMessage>(decode(frames.nextFrame())).id)
                assertEquals(cancel(RequestId.of(1)), decode(frames.nextFrame()))
                assertEquals(0, connection.dispatcher.pending.count)
                connection.close()
            }
        }

    @Test
    fun `answered request sends no cancel request`() =
        runTest {
            for (connect in dispatchModes) {
                val connection = connect(backgroundScope)
                val frames = backgroundScope.outgoingFrames(connection)
                val call = backgroundScope.async { connection.request<JsonElement>("echo", null) }
                val sent = assertIs<RequestMessage>(decode(frames.nextFrame()))
                connection.receive(codec.encode(ResponseMessage.success(sent.id, JsonPrimitive("ok"))))
                assertEquals(JsonPrimitive("ok"), call.await())
                runCurrent()
                assertTrue(frames.tryReceive().isFailure)
                connection.close()
            }
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

    private fun decode(frame: ByteArray): Message = JsonRpcMessageDecoder(codec).feed(frame).messages().single()

    private fun cancel(id: RequestId): NotificationMessage {
        val wireId =
            when (id) {
                is RequestId.NumberId -> JsonPrimitive(id.value)
                is RequestId.StringId -> JsonPrimitive(id.value)
            }
        return NotificationMessage("$/cancelRequest", JsonObject(mapOf("id" to wireId)))
    }
}
