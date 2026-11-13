package io.jsonrpc4k.transport

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
}

/**
 * Create a platform-specific stdio transport.
 */
internal expect fun createStdioTransport(): Transport

// Windows named pipes do not exist on POSIX, and the Windows native target has no validated
// implementation yet, so no native target supports them.
internal actual suspend fun connectNamedPipe(pipePath: String): Transport =
    throw UnsupportedTransportException("Named pipes are not supported on native targets: $pipePath")

internal actual suspend fun bindNamedPipe(pipePath: String): LocalListener =
    throw UnsupportedTransportException("Named pipes are not supported on native targets: $pipePath")

internal actual fun connectNodeIpc(): Transport =
    throw UnsupportedTransportException("Node IPC exists only in Node.js processes started with an IPC channel")
