package io.jsonrpc4k.core

/**
 * Opts a connection into bounded asynchronous request dispatch.
 *
 * Notifications are barriers: all earlier requests finish before a notification runs, and
 * later application messages wait for it. Responses and cancellation bypass that queue.
 * A request that awaits a chain of nested callbacks can still exhaust the concurrency limit.
 * A request arriving when the queue is full receives a server-busy error. A notification
 * overflow, duplicate active incoming ID, or inability to enqueue an error closes the
 * connection with a [ConnectionState.Failed] cause. Malformed cancellation notifications
 * are ignored. Cancellation applies only to an accepted incoming request.
 * An incoming ID remains active until its response is dequeued by the outgoing collector;
 * this local handoff does not establish transport completion or peer receipt.
 *
 * @param maxConcurrentRequests Maximum concurrently running request handlers, from 1 to 64.
 * @param maxQueuedMessages Maximum queued requests and notifications, from 1 to 1024.
 */
public data class BoundedDispatchPolicy(
    val maxConcurrentRequests: Int,
    val maxQueuedMessages: Int,
) {
    init {
        require(maxConcurrentRequests in 1..MAX_CONCURRENT_REQUESTS) { "maxConcurrentRequests must be between 1 and 64" }
        require(maxQueuedMessages in 1..MAX_QUEUED_MESSAGES) { "maxQueuedMessages must be between 1 and 1024" }
    }

    private companion object {
        const val MAX_CONCURRENT_REQUESTS = 64
        const val MAX_QUEUED_MESSAGES = 1024
    }
}

/** Terminal state for a connection; concurrent terminal events use the first observed transition. */
public sealed interface ConnectionState {
    /** The connection is open. */
    public data object Open : ConnectionState

    /** The connection was closed or its parent was canceled. */
    public data object Closed : ConnectionState

    /** The connection failed; [cause] retains the original failure. */
    public data class Failed(val cause: Throwable) : ConnectionState
}
