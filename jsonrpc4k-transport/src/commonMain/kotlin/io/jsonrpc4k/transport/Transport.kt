package io.jsonrpc4k.transport

import kotlinx.coroutines.flow.Flow

/**
 * A bidirectional, byte-transparent transport for JSON-RPC communication.
 *
 * A transport moves opaque byte chunks. It never frames, parses, splits or merges messages: chunk
 * boundaries on [incoming] are arbitrary and need not match the boundaries of [send] calls.
 *
 * ## Contract
 *
 * - **Single collector.** [incoming] exposes a single stream of bytes. At most one
 *   collector may be active at a time; concurrent collectors split the data between them and the
 *   result is unspecified. Sessions collect it exactly once.
 * - **EOF.** [incoming] completes normally when the peer closes its writing side (EOF) and
 *   exceptionally with [TransportException] when the underlying I/O fails while the transport is
 *   open. EOF does not release resources: call [close] afterwards. [isConnected] is `false` after
 *   EOF.
 * - **Close.** [close] is idempotent, never blocks indefinitely on a live peer, and is safe to call
 *   from a cancelled coroutine. Closing the transport signals EOF to the peer and makes a pending
 *   [incoming] collection complete quietly (no exception). [isConnected] is `false` after close.
 * - **Send.** [send] writes the whole chunk or fails; concurrent calls are serialized. After close,
 *   or after the peer has gone away, it throws [TransportException]. Platforms that cannot provide
 *   a transport kind at all throw [UnsupportedTransportException].
 * - **Ownership.** A session never closes the transport it was given; whoever created the transport
 *   closes it.
 */
public interface Transport {
    /**
     * Flow of incoming byte chunks. See the contract on [Transport]: single collector, completes
     * normally on EOF or [close], fails with [TransportException] on I/O errors.
     */
    public val incoming: Flow<ByteArray>

    /**
     * Send a chunk of bytes over the transport.
     *
     * @throws TransportException if the transport is closed, the peer is gone or the write fails.
     */
    public suspend fun send(data: ByteArray)

    /**
     * Close the transport. Idempotent; signals EOF to the peer and ends [incoming].
     */
    public suspend fun close()

    /**
     * Whether the transport is currently connected/open. `false` after EOF or [close].
     */
    public val isConnected: Boolean
}

/**
 * Factory for transports that need no connection setup. Connecting transports are suspending:
 * see [TcpSocket], [UnixSocket], [NamedPipe], [NodeIpc] and the [connect] helper for LSP launch
 * arguments.
 */
public expect object TransportFactory {
    /**
     * Create a stdio transport (stdin/stdout).
     */
    public fun stdio(): Transport
}

/**
 * Exception thrown when transport operations fail.
 */
public open class TransportException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)

/**
 * Thrown when a transport kind is not available on the current platform, for example a TCP listener
 * in a browser.
 */
public class UnsupportedTransportException(
    message: String,
    cause: Throwable? = null,
) : TransportException(message, cause)

/**
 * A connected JSON-RPC session over a transport.
 */
public interface JsonRpcSession {
    /**
     * The underlying transport.
     */
    public val transport: Transport

    /**
     * Start the session - begins reading from transport and processing messages.
     */
    public suspend fun start()

    /**
     * Stop the session and close the transport.
     */
    public suspend fun stop()
}
