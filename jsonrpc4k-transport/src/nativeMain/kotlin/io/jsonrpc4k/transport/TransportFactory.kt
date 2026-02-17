package io.jsonrpc4k.transport

import kotlinx.coroutines.runBlocking

/**
 * Native implementation of TransportFactory.
 *
 * Platform-specific transport implementations are provided by posixMain (macOS, Linux)
 * and mingwMain (Windows).
 */
public actual object TransportFactory {
    /**
     * Create a stdio transport.
     */
    public actual fun stdio(): Transport = createStdioTransport()

    /**
     * Create a socket transport that connects to the given host and port.
     *
     * Note: This is a blocking call.
     */
    public actual fun socket(
        host: String,
        port: Int,
    ): Transport = runBlocking { createSocketTransport(host, port) }
}

/**
 * Create a platform-specific stdio transport.
 */
internal expect fun createStdioTransport(): Transport

/**
 * Create a platform-specific socket transport.
 */
internal expect suspend fun createSocketTransport(
    host: String,
    port: Int,
): Transport
