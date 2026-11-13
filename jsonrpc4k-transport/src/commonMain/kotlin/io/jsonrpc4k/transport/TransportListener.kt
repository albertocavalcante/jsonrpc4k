package io.jsonrpc4k.transport

/**
 * A server endpoint that hands out one [Transport] per accepted connection.
 *
 * Ownership: the listener owns its server socket; every [Transport] returned by [accept] is owned
 * by the caller and keeps working after the listener is closed.
 */
public interface TransportListener {
    /**
     * The local port the listener is bound to. When bound with port 0 this is the port the
     * operating system chose. `-1` for a [LocalListener], which has no TCP port.
     */
    public val port: Int

    /**
     * Wait for the next connection and return its transport.
     *
     * Fails with [TransportException] when the listener is closed, including when it is closed while
     * this call is waiting. Cancelling the calling coroutine abandons the wait without closing the
     * listener, except on Windows where only [close] unblocks a pending accept.
     */
    public suspend fun accept(): Transport

    /**
     * Stop listening. Idempotent. A pending [accept] fails promptly; connections that were already
     * accepted are unaffected.
     */
    public suspend fun close()
}

/**
 * A [TransportListener] on a local IPC endpoint: a Unix domain socket or a Windows named pipe.
 * It has no TCP port, so [TransportListener.port] is `-1`.
 */
public interface LocalListener : TransportListener {
    /** The socket path, or the full Windows pipe path, the listener is bound to. */
    public val path: String
}

/**
 * Entry point for TCP listeners: `TcpListener.bind("127.0.0.1", 0)`.
 *
 * Available on JVM, Node.js, POSIX native and Windows native. In a browser [bind] throws
 * [UnsupportedTransportException].
 */
public object TcpListener {
    /**
     * Bind and listen on [host]:[port]. Use port 0 to let the operating system choose a free port
     * and read it back from [TransportListener.port].
     *
     * @throws TransportException if the address cannot be resolved or bound.
     * @throws UnsupportedTransportException on platforms without TCP servers.
     */
    public suspend fun bind(
        host: String,
        port: Int,
    ): TransportListener = bindTcpListener(host, port)
}

internal expect suspend fun bindTcpListener(
    host: String,
    port: Int,
): TransportListener
