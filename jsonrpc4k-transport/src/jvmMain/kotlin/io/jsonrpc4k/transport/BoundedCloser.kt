package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeoutOrNull
import java.io.Closeable
import kotlin.concurrent.thread
import kotlin.time.Duration

/**
 * Closes one owned resource on a daemon thread and waits a bounded time for it.
 *
 * Closing a stream can block indefinitely, for example a process pipe or console whose blocked reader or writer
 * holds the stream's lock. Such a close keeps running in the background after [close] times out, and the next
 * [close] observes its outcome instead of starting another one. Callers serialize calls.
 */
internal class BoundedCloser(
    private val resource: Closeable,
    private val threadName: String,
) {
    private var closed = false
    private var attempt: CompletableDeferred<Throwable?>? = null

    /**
     * Returns the failure of a finished close attempt, or `null` when the resource is closed or is still closing
     * after [timeout]. A resource whose close failed is closed again by the next call.
     */
    suspend fun close(timeout: Duration): Throwable? {
        if (closed) return null
        val current = attempt ?: start().also { attempt = it }
        if (withTimeoutOrNull(timeout) { current.join() } == null) return null
        attempt = null
        val failure = current.await()
        closed = failure == null
        return failure
    }

    private fun start(): CompletableDeferred<Throwable?> {
        val outcome = CompletableDeferred<Throwable?>()
        thread(isDaemon = true, name = threadName) {
            outcome.complete(closeCatching())
        }
        return outcome
    }

    private fun closeCatching(): Throwable? =
        try {
            resource.close()
            null
        } catch (failure: Throwable) {
            failure
        }
}
