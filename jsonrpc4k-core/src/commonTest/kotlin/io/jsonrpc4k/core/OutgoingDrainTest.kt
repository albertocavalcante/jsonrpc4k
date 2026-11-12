package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertSame
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class OutgoingDrainTest {
    @Test
    fun `empty flush completes with an active collector`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            backgroundScope.launch { connection.outgoing.collect {} }
            withTimeout(1.seconds) { connection.flushOutgoing() }
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `flush waits for preceding sends and preserves frame order`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val received = mutableListOf<String>()
            backgroundScope.launch {
                connection.outgoing.collect { frame ->
                    received.add(frame.decodeToString())
                    if (received.size == 1) {
                        entered.complete(Unit)
                        release.await()
                    }
                }
            }
            connection.notify("first", null)
            connection.notify("second", null)
            entered.await()
            val flush = async { connection.flushOutgoing() }
            runCurrent()
            assertFalse(flush.isCompleted)
            release.complete(Unit)
            withTimeout(1.seconds) { flush.await() }
            assertEquals(2, received.size)
            assertTrue(received[0].contains("\"first\""))
            assertTrue(received[1].contains("\"second\""))
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `downstream failure fails queued and future flushes with original cause`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val failure = IllegalArgumentException("send failed")
            backgroundScope.launch {
                runCatching {
                    connection.outgoing.collect {
                        entered.complete(Unit)
                        release.await()
                        throw failure
                    }
                }
            }
            connection.notify("first", null)
            entered.await()
            val flush = async { runCatching { connection.flushOutgoing() } }
            runCurrent()
            assertFalse(flush.isCompleted)
            release.complete(Unit)
            val first = withTimeout(1.seconds) { flush.await() }.exceptionOrNull()
            assertTrue(first is IllegalStateException)
            assertSame(failure, first.cause)
            val later = withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.flushOutgoing() } }
            assertSame(failure, later.cause)
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `cancelled flush unregisters while enqueue is blocked`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            repeat(64) { connection.notify("fill", null) }
            val flush = launch { connection.flushOutgoing() }
            runCurrent()
            assertEquals(1, connection.pendingFlushCount)
            flush.cancelAndJoin()
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `downstream failure wakes a flush blocked on a full channel`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val failure = IllegalArgumentException("send failed")
            backgroundScope.launch {
                runCatching {
                    connection.outgoing.collect {
                        entered.complete(Unit)
                        release.await()
                        throw failure
                    }
                }
            }
            connection.notify("first", null)
            entered.await()
            repeat(64) { connection.notify("fill", null) }
            val flush = async { runCatching { connection.flushOutgoing() } }
            runCurrent()
            assertFalse(flush.isCompleted)
            release.complete(Unit)
            val error = withTimeout(1.seconds) { flush.await() }.exceptionOrNull()
            assertTrue(error is IllegalStateException)
            assertSame(failure, error.cause)
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `close cancels a flush without a collector`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val flush = async { runCatching { connection.flushOutgoing() } }
            runCurrent()
            assertEquals(1, connection.pendingFlushCount)
            connection.close()
            assertTrue(withTimeout(1.seconds) { flush.await() }.isFailure)
            assertEquals(0, connection.pendingFlushCount)
        }

    @Test
    fun `close cancels a flush blocked on a full channel`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            repeat(64) { connection.notify("fill", null) }
            val flush = async { runCatching { connection.flushOutgoing() } }
            runCurrent()
            assertEquals(1, connection.pendingFlushCount)
            connection.close()
            assertTrue(withTimeout(1.seconds) { flush.await() }.isFailure)
            assertEquals(0, connection.pendingFlushCount)
        }

    @Test
    fun `cancelled queued barrier does not block later flush`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val abandoned = launch { connection.flushOutgoing() }
            runCurrent()
            abandoned.cancelAndJoin()
            assertEquals(0, connection.pendingFlushCount)
            backgroundScope.launch { connection.outgoing.collect {} }
            withTimeout(1.seconds) { connection.flushOutgoing() }
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `only one outgoing collector is active and first remains usable`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val first = backgroundScope.async { connection.outgoing.first() }
            runCurrent()
            withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.outgoing.first() } }
            connection.notify("first", null)
            assertTrue(first.await().decodeToString().contains("\"first\""))
            connection.notify("second", null)
            assertTrue(connection.outgoing.first().decodeToString().contains("\"second\""))
            withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.flushOutgoing() } }
            connection.close()
        }

    @Test
    fun `cancelled receiving collector cannot make a later flush succeed`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val oldCollector = backgroundScope.launch { connection.outgoing.collect {} }
            runCurrent()
            connection.notify("possibly-lost", null)
            oldCollector.cancelAndJoin()
            backgroundScope.launch { connection.outgoing.collect {} }
            runCurrent()
            withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.flushOutgoing() } }
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `cancelled receiving collector fails a barrier removed before dispatch`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val collector = backgroundScope.launch { connection.outgoing.collect {} }
            runCurrent()
            val flush = async(start = CoroutineStart.UNDISPATCHED) { runCatching { connection.flushOutgoing() } }
            assertEquals(1, connection.pendingFlushCount)
            collector.cancelAndJoin()
            assertTrue(withTimeout(1.seconds) { flush.await() }.isFailure)
            assertEquals(0, connection.pendingFlushCount)
            connection.close()
        }

    @Test
    fun `cancelled idle collector conservatively disables flush`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val collector = backgroundScope.launch { connection.outgoing.collect {} }
            runCurrent()
            collector.cancelAndJoin()
            withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.flushOutgoing() } }
            connection.close()
        }

    @Test
    fun `take still receives frames but conservatively disables flush`() =
        runTest {
            val connection = Connection(scope = backgroundScope)
            val frames = backgroundScope.async { connection.outgoing.take(2).toList() }
            connection.notify("first", null)
            connection.notify("second", null)
            assertEquals(2, frames.await().size)
            withTimeout(1.seconds) { assertFailsWith<IllegalStateException> { connection.flushOutgoing() } }
            connection.close()
        }

    @Test
    fun `parent cancellation terminates an idle flush`() =
        runTest {
            val parent = Job()
            val connection = Connection(scope = CoroutineScope(parent))
            val flush = async { runCatching { connection.flushOutgoing() } }
            runCurrent()
            assertEquals(1, connection.pendingFlushCount)
            parent.cancel()
            assertTrue(withTimeout(1.seconds) { flush.await() }.isFailure)
            assertEquals(0, connection.pendingFlushCount)
        }
}
