package io.jsonrpc4k.transport

private const val UNSUPPORTED =
    "The JVM has no full-duplex named pipe support without native code (JNI or the foreign function API); " +
        "use TCP or a Unix socket, or run on Node.js"

internal actual suspend fun connectNamedPipe(pipePath: String): Transport =
    throw UnsupportedTransportException("$UNSUPPORTED: $pipePath")

internal actual suspend fun bindNamedPipe(pipePath: String): LocalListener =
    throw UnsupportedTransportException("$UNSUPPORTED: $pipePath")
