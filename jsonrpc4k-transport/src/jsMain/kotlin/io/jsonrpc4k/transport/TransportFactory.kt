package io.jsonrpc4k.transport

/**
 * JS implementation of TransportFactory for Node.js.
 *
 * Provides the stdio transport using Node.js APIs.
 * Note: This implementation requires a Node.js environment and will not work in browsers.
 */
public actual object TransportFactory {
    /**
     * Create a stdio transport using Node.js process.stdin/stdout.
     *
     * Note: This is only available in Node.js environments.
     */
    public actual fun stdio(): Transport = StdioTransport()
}
