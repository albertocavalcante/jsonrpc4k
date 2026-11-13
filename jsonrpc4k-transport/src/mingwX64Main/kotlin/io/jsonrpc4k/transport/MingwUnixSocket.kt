package io.jsonrpc4k.transport

// Windows 10 offers AF_UNIX, but the Kotlin/Native Windows bindings do not expose sockaddr_un and no
// Windows runner validates it, so it stays unsupported instead of shipping unproven code.
internal actual suspend fun connectUnixSocket(path: String): Transport =
    throw UnsupportedTransportException("Unix domain sockets are not supported on Windows native: $path")

internal actual suspend fun bindUnixSocket(path: String): LocalListener =
    throw UnsupportedTransportException("Unix domain sockets are not supported on Windows native: $path")
