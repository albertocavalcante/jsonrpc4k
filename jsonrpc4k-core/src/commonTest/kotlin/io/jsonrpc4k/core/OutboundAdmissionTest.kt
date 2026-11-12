package io.jsonrpc4k.core

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class OutboundAdmissionTest {
    @Test
    fun `assigned then cancelled waiter restores token for the next waiter`() = runTest {
        var failure: Throwable? = null
        val gate = OutboundAdmission(CompletableDeferred()) { failure = it }
        gate.acquire()
        var entered = false
        val cancelled = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
            gate.acquire()
            entered = true
            gate.release()
        }
        gate.release()
        cancelled.cancel()
        runCurrent()
        assertFalse(entered)
        val next = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) { gate.acquire() }
        withTimeout(1000) { next.await() }
        gate.release()
        assertEquals(null, failure)
    }

    @Test
    fun `cancelled owner returns token and terminal wakes blocked waiter`() = runTest {
        var failure: Throwable? = null
        val ended = CompletableDeferred<Unit>()
        val gate = OutboundAdmission(ended) { failure = it }
        val entered = CompletableDeferred<Unit>()
        val owner = backgroundScope.launch(start = CoroutineStart.UNDISPATCHED) {
            gate.acquire()
            try {
                entered.complete(Unit)
                CompletableDeferred<Unit>().await()
            } finally {
                gate.release()
            }
        }
        withTimeout(1000) { entered.await() }
        val waiter = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { gate.acquire() }
        }
        val cause = IllegalStateException("closed")
        ended.completeExceptionally(cause)
        assertTrue(withTimeout(1000) { waiter.await() }.isFailure)
        owner.cancel()
        runCurrent()
        assertEquals(null, failure)
    }

    @Test
    fun `unexpected normal terminal signal cannot create a token`() = runTest {
        var failure: Throwable? = null
        val ended = CompletableDeferred<Unit>()
        val gate = OutboundAdmission(ended) { failure = it }
        gate.acquire()
        val waiter = backgroundScope.async(start = CoroutineStart.UNDISPATCHED) {
            runCatching { gate.acquire() }
        }
        ended.complete(Unit)
        assertTrue(withTimeout(1000) { waiter.await() }.isFailure)
        gate.release()
        assertEquals(null, failure)
    }

}
