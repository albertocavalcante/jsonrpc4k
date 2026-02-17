package io.jsonrpc4k.transport

/**
 * JS implementation of TransportFactory for Node.js.
 *
 * Provides stdio and socket transports using Node.js APIs.
 * Note: These implementations require a Node.js environment and will not work in browsers.
 */
public actual object TransportFactory {
    /**
     * Create a stdio transport using Node.js process.stdin/stdout.
     *
     * Note: This is only available in Node.js environments.
     */
    public actual fun stdio(): Transport = StdioTransport()

    /**
     * Create a socket transport that connects to the given host and port.
     *
     * Note: In JavaScript, socket connections are inherently asynchronous.
     * This synchronous factory method cannot be properly implemented in JS.
     *
     * Use [SocketTransport.connect] instead for proper async socket creation:
     * ```kotlin
     * val transport = SocketTransport.connect(host, port)
     * ```
     *
     * @throws TransportException Always, indicating that the async API should be used instead.
     */
    public actual fun socket(
        host: String,
        port: Int,
    ): Transport =
        throw TransportException(
            "Synchronous socket creation is not supported in JavaScript. " +
                "Use SocketTransport.connect(host, port) in a coroutine context instead.",
        )
}
