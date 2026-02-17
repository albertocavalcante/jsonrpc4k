package io.jsonrpc4k.transport

import kotlinx.coroutines.flow.Flow

/**
 * A bidirectional transport for JSON-RPC communication.
 *
 * Implementations handle the platform-specific I/O (stdio, sockets, etc.)
 * while this interface provides a common abstraction.
 */
public interface Transport {
    /**
     * Flow of incoming data from the transport.
     */
    public val incoming: Flow<ByteArray>

    /**
     * Send data over the transport.
     */
    public suspend fun send(data: ByteArray)

    /**
     * Close the transport.
     */
    public suspend fun close()

    /**
     * Whether the transport is currently connected/open.
     */
    public val isConnected: Boolean
}

/**
 * Factory for creating transports.
 */
public expect object TransportFactory {
    /**
     * Create a stdio transport (stdin/stdout).
     */
    public fun stdio(): Transport

    /**
     * Create a socket transport that connects to the given host and port.
     */
    public fun socket(
        host: String,
        port: Int,
    ): Transport
}

/**
 * Exception thrown when transport operations fail.
 */
public class TransportException(
    message: String,
    cause: Throwable? = null,
) : Exception(message, cause)

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
