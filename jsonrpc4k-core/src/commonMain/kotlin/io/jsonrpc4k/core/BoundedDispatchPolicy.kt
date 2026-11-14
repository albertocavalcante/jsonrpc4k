package io.jsonrpc4k.core

import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Opts a connection into bounded asynchronous request dispatch.
 *
 * Notifications are barriers: all earlier requests finish before a notification runs, and
 * later application messages wait for it. Responses and cancellation bypass that queue, and so
 * do the notifications named in [controlNotifications]. A request that awaits a chain of nested
 * callbacks can still exhaust the concurrency limit. A request arriving when the queue is full
 * receives a server-busy error. A notification overflow, duplicate active incoming ID, or
 * inability to enqueue an error closes the connection with a [ConnectionState.Failed] cause.
 * Malformed cancellation notifications are ignored. Cancellation applies only to an accepted
 * incoming request. An incoming ID remains active until its response is dequeued by the outgoing
 * collector; this local handoff does not establish transport completion or peer receipt.
 *
 * Each entry of a JSON-RPC 2.0 batch is dispatched as its own message under these rules and
 * limits: a request entry takes a queue slot and a concurrency permit, can be cancelled, and is
 * answered with server busy when the queue is full, while a notification entry is a barrier or a
 * control notification. The batch is still answered with exactly one array, sent once every entry
 * has finished, and with nothing when it holds no request and no invalid entry. The rules that fail
 * the connection apply per entry too: a batch whose notification entries outnumber the free queue
 * slots, or whose request entries repeat an id, among themselves or with any request still active,
 * closes the connection with a [ConnectionState.Failed] cause instead of being answered.
 *
 * @param maxConcurrentRequests Maximum concurrently running request handlers, from 1 to 64.
 * @param maxQueuedMessages Maximum queued requests and notifications, from 1 to 1024.
 * @param drainTimeout How long [Connection.endInput] waits for admitted work after the end of
 * input before it cancels what remains; finite and not negative.
 * @param controlNotifications Notification methods that bypass the barrier, such as
 * `window/workDoneProgress/cancel` or `run/readStdin`. Each is dispatched as soon as it is
 * admitted, in arrival order among themselves, on one lane that runs beside the request handlers,
 * so a running handler can observe it. They still count toward [maxQueuedMessages] and the inbound
 * byte budget while they wait. `$/cancelRequest` keeps its own handling whether or not it is listed.
 * A connection copies this set when it is created, so changing a mutable set afterwards does not
 * affect it; as in any data class, though, equality and the hash code read the set as it is now,
 * so pass an immutable set.
 */
public data class BoundedDispatchPolicy(
    val maxConcurrentRequests: Int,
    val maxQueuedMessages: Int,
    val drainTimeout: Duration = DEFAULT_DRAIN_TIMEOUT,
    val controlNotifications: Set<String> = emptySet(),
) {
    init {
        require(maxConcurrentRequests in 1..MAX_CONCURRENT_REQUESTS) { "maxConcurrentRequests must be between 1 and 64" }
        require(maxQueuedMessages in 1..MAX_QUEUED_MESSAGES) { "maxQueuedMessages must be between 1 and 1024" }
        require(drainTimeout.isFinite() && !drainTimeout.isNegative()) { "drainTimeout must be finite and not negative" }
    }

    private companion object {
        const val MAX_CONCURRENT_REQUESTS = 64
        const val MAX_QUEUED_MESSAGES = 1024
        val DEFAULT_DRAIN_TIMEOUT = 5.seconds
    }
}

/** Terminal state for a connection; concurrent terminal events use the first observed transition. */
public sealed interface ConnectionState {
    /** The connection is open. */
    public data object Open : ConnectionState

    /** The connection was closed or its parent was canceled. */
    public data object Closed : ConnectionState

    /** The connection failed; [cause] retains the original failure. */
    public data class Failed(
        val cause: Throwable,
    ) : ConnectionState
}
