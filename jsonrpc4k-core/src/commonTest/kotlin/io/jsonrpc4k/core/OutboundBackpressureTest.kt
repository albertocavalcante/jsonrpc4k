package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.test.assertSame
import kotlin.time.Duration.Companion.milliseconds

class OutboundBackpressureTest {
    private fun connection() = Connection(
        BoundedDispatchPolicy(1, 2),
        InboundResourceLimits(65536, 32, 65536),
        OutboundResourceLimits(65536, 65536, 1, 8, 65536, 2, 10000, 32),
    )

    @Test
    fun `writer holds capacity until emit returns and producers resume in order`() = runTest {
        val connection = connection()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val frames = mutableListOf<String>()
        val writer = backgroundScope.launch {
            connection.outgoing.collect {
                frames.add(it.decodeToString())
                entered.complete(Unit)
                release.await()
            }
        }
        connection.notify("first", null)
        entered.await()
        val second = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.notify("second", null) }
        val third = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.notify("third", null) }
        assertFalse(second.isCompleted)
        assertFalse(third.isCompleted)
        release.complete(Unit)
        withTimeout(1000) { second.await(); third.await() }
        runCurrent()
        assertEquals(3, frames.size)
        assertTrue(frames[0].contains("first"))
        assertTrue(frames[1].contains("second"))
        assertTrue(frames[2].contains("third"))
        connection.close()
        writer.cancel()
    }

    @Test
    fun `closing releases active byte waiter and queued mutex waiters`() = runTest {
        val connection = connection()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val writer = backgroundScope.launch {
            connection.outgoing.collect {
                entered.complete(Unit)
                release.await()
            }
        }
        connection.notify("first", JsonPrimitive("payload"))
        entered.await()
        val waiters = (1..4).map { n ->
            backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { connection.notify("waiter$n", null) }
            }
        }
        assertTrue(waiters.none { it.isCompleted })
        connection.close()
        withTimeout(1000) { waiters.forEach { assertTrue(it.await().isFailure) } }
        release.complete(Unit)
        writer.cancel()
    }

    @Test
    fun `cancelled waiter does not block later admission or flush`() = runTest {
        val connection = connection()
        val entered = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val writer = backgroundScope.launch {
            connection.outgoing.collect {
                entered.complete(Unit)
                release.await()
            }
        }
        connection.notify("first", null)
        entered.await()
        val cancelled = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.notify("cancelled", null) }
        val next = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.notify("next", null) }
        val flush = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { connection.flushOutgoing() }
        cancelled.cancel()
        release.complete(Unit)
        withTimeout(1000) { next.await(); flush.await() }
        connection.close()
        writer.cancel()
    }

    @Test
    fun `oversized public message fails without closing connection`() = runTest {
        val connection = connection()
        val result = runCatching { connection.notify("large", JsonPrimitive("a".repeat(70000))) }
        assertTrue(result.isFailure)
        assertEquals(ConnectionState.Open, connection.state.value)
        connection.close()
    }


    @Test
    fun `reserved control frame bypasses full ordinary capacity and saturation fails`() = runTest {
        var failure: Throwable? = null
        val limits = OutboundResourceLimits(1024, 1024, 1, 4, 1024, 1, 1000, 32)
        val queue = BoundedOutgoing(limits) { failure = it }
        queue.send(NotificationMessage("ordinary"))
        val response = ResponseMessage.error(RequestId.of(1), ResponseError.requestCancelled())
        assertEquals(OfferResult.ACCEPTED, queue.offer(response, null))
        assertEquals(OfferResult.REJECTED, queue.offer(response, null))
        assertTrue(failure is IllegalStateException)
        val first = queue.items.receive()
        val second = queue.items.receive()
        assertTrue(first is BoundedOutgoing.Item.Frame)
        assertTrue(second is BoundedOutgoing.Item.Frame)
        assertTrue(first.bytes.decodeToString().contains("ordinary"))
        first.lease.release()
        second.lease.release()
        queue.close(IllegalStateException("closed"))
    }


    @Test
    fun `request timeout includes outbound admission`() = runTest {
        val connection = connection()
        connection.notify("first", null)
        assertFailsWith<JsonRpcException> {
            connection.request<JsonElement>("blocked", null, 100.milliseconds)
        }
        assertEquals(ConnectionState.Open, connection.state.value)
        connection.close()
    }


    @Test
    fun `control accepted before waiting barrier is delivered first`() = runTest {
        val limits = OutboundResourceLimits(1024, 1024, 1, 4, 1024, 1, 1000, 32)
        val queue = BoundedOutgoing(limits) { throw it }
        val drain = OutgoingDrain()
        queue.send(NotificationMessage("ordinary"))
        val marker = CompletableDeferred<Unit>()
        val barrier = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { queue.barrier(marker, drain) }
        assertFalse(barrier.isCompleted)
        assertEquals(OfferResult.ACCEPTED, queue.offer(ResponseMessage.error(RequestId.of(1), ResponseError.requestCancelled()), null))
        val first = queue.items.receive()
        first.lease.release()
        barrier.await()
        val control = queue.items.receive()
        assertTrue(control is BoundedOutgoing.Item.Frame)
        control.lease.release()
        val last = queue.items.receive()
        assertTrue(last is BoundedOutgoing.Item.Barrier)
        last.lease.release()
        drain.unregister(marker)
        queue.close(IllegalStateException("closed"))
    }


    @Test
    fun `parent cancellation wakes byte and mutex waiters`() = runTest {
        val parent = Job()
        val connection = Connection(
            BoundedDispatchPolicy(1, 2),
            InboundResourceLimits(65536, 32, 65536),
            OutboundResourceLimits(65536, 65536, 1, 8, 65536, 2, 10000, 32),
            scope = CoroutineScope(parent + StandardTestDispatcher(testScheduler)),
        )
        connection.notify("first", null)
        val waiters = (1..4).map { n ->
            backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { connection.notify("waiter$n", null) }
            }
        }
        parent.cancel()
        withTimeout(1000) { waiters.forEach { assertTrue(it.await().isFailure) } }
        assertEquals(ConnectionState.Closed, connection.state.value)
    }


    @Test
    fun `large valid frame is admitted at exact wire ceiling`() = runTest {
        val message = NotificationMessage("large", JsonPrimitive("é\\\"".repeat(8192)))
        val expected = JsonRpcCodec.Default.encode(message)
        val limits = OutboundResourceLimits(expected.size, expected.size, 1, 1, 1024, 1, 1000, 32)
        val queue = BoundedOutgoing(limits) { throw it }
        queue.send(message)
        val frame = queue.items.receive()
        assertTrue(frame is BoundedOutgoing.Item.Frame)
        assertTrue(expected.contentEquals(frame.bytes))
        frame.lease.release()
        queue.close(IllegalStateException("closed"))
    }


    @Test
    fun `already cancelled parent prevents admission`() = runTest {
        val parent = Job().apply { cancel() }
        val connection = Connection(
            BoundedDispatchPolicy(1, 2),
            InboundResourceLimits(65536, 32, 65536),
            OutboundResourceLimits(65536, 65536, 1, 8, 65536, 2, 10000, 32),
            scope = CoroutineScope(parent + StandardTestDispatcher(testScheduler)),
        )
        assertTrue(runCatching { connection.notify("late", null) }.isFailure)
        assertEquals(ConnectionState.Closed, connection.state.value)
    }


    @Test
    fun `writer failure wakes all admissions and preserves original cause`() = runTest {
        val connection = connection()
        val entered = CompletableDeferred<Unit>()
        val failWriter = CompletableDeferred<Unit>()
        val original = IllegalStateException("transport failed")
        val writer = backgroundScope.async {
            runCatching {
                connection.outgoing.collect {
                    entered.complete(Unit)
                    failWriter.await()
                    throw original
                }
            }
        }
        connection.notify("first", null)
        entered.await()
        val waiters = (1..4).map { n ->
            backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
                runCatching { connection.notify("waiter$n", null) }
            }
        }
        failWriter.complete(Unit)
        withTimeout(1000) { writer.await(); waiters.forEach { assertTrue(it.await().isFailure) } }
        val failed = connection.state.value
        assertTrue(failed is ConnectionState.Failed)
        assertSame(original, failed.cause)
    }


    @Test
    fun `cancelled assigned receive blocks new admission until terminal cause`() = runTest {
        val limits = OutboundResourceLimits(1024, 1024, 1, 4, 1024, 1, 1000, 32)
        val queue = BoundedOutgoing(limits) { throw it }
        val receiver = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { queue.items.receive() }
        queue.send(NotificationMessage("assigned"))
        receiver.cancel()
        val waiter = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { queue.send(NotificationMessage("later")) }
        }
        runCurrent()
        assertFalse(waiter.isCompleted)
        assertEquals(OfferResult.TERMINAL_TRANSITION, queue.offer(ResponseMessage.error(RequestId.of(7), ResponseError.requestCancelled()), null))
        assertTrue(queue.items.tryReceive().isFailure)
        queue.close(IllegalStateException("collector interrupted"))
        assertTrue(withTimeout(1000) { waiter.await() }.isFailure)
    }


    @Test
    fun `lost delivery wins control arbitration and preserves collector cause`() = runTest {
        var halted = false
        var failure: Throwable? = null
        val limits = OutboundResourceLimits(1024, 1024, 1, 4, 1024, 1, 1000, 32)
        val queue = BoundedOutgoing(limits, haltIngress = { halted = true }, fail = { failure = it })
        val receiver = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { queue.items.receive() }
        queue.send(NotificationMessage("assigned"))
        receiver.cancel()
        runCurrent()
        assertTrue(halted)
        assertEquals(0, queue.outstandingCount)
        assertEquals(0, queue.outstandingBytes)
        assertEquals(OfferResult.TERMINAL_TRANSITION, queue.offer(ResponseMessage.error(RequestId.of(8), ResponseError.requestCancelled()), null))
        assertEquals(null, failure)
        val original = IllegalStateException("collector")
        queue.resolveCollectorLoss(original)
        assertTrue(queue.claimFailure(original))
        assertFalse(queue.claimFailure(IllegalStateException("other")))
        queue.close(original)
    }

    @Test
    fun `control saturation wins before assigned receiver is cancelled`() = runTest {
        var halted = false
        var failure: Throwable? = null
        val limits = OutboundResourceLimits(1024, 1024, 1, 4, 1024, 1, 1000, 32)
        val queue = BoundedOutgoing(limits, haltIngress = { halted = true }, fail = { failure = it })
        val response = ResponseMessage.error(RequestId.of(9), ResponseError.requestCancelled())
        val receiver = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) { queue.items.receive() }
        assertEquals(OfferResult.ACCEPTED, queue.offer(response, null))
        assertEquals(OfferResult.REJECTED, queue.offer(response, null))
        val winner = requireNotNull(failure)
        assertTrue(halted)
        halted = false
        receiver.cancel()
        runCurrent()
        assertFalse(halted)
        assertEquals(0, queue.outstandingCount)
        assertEquals(0, queue.outstandingBytes)
        assertTrue(queue.claimFailure(winner))
        assertFalse(queue.claimFailure(IllegalStateException("collector")))
        queue.close(winner)
    }


    @Test
    fun `cancelled writer with assigned frame closes public connection`() = runTest {
        val connection = connection()
        val writer = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
            connection.outgoing.collect { error("assigned frame must not be delivered") }
        }
        connection.notify("assigned", null)
        writer.cancel()
        runCurrent()
        assertTrue(connection.state.value is ConnectionState.Failed)
        assertTrue(runCatching { connection.notify("later", null) }.isFailure)
        connection.close()
    }


    @Test
    fun `flush waiter closing on failure cannot overwrite writer cause`() = runTest {
        val connection = connection()
        val entered = CompletableDeferred<Unit>()
        val failWriter = CompletableDeferred<Unit>()
        val original = IllegalStateException("writer failed")
        val writer = backgroundScope.async {
            runCatching {
                connection.outgoing.collect {
                    entered.complete(Unit)
                    failWriter.await()
                    throw original
                }
            }
        }
        connection.notify("first", null)
        entered.await()
        val flush = backgroundScope.async(Dispatchers.Unconfined) {
            try {
                runCatching { connection.flushOutgoing() }
            } finally {
                connection.close()
            }
        }
        failWriter.complete(Unit)
        withTimeout(1000) { writer.await(); flush.await() }
        val failed = connection.state.value
        assertTrue(failed is ConnectionState.Failed)
        assertSame(original, failed.cause)
    }

}
