package io.jsonrpc4k.transport

internal actual fun connectNodeIpc(): Transport =
    throw UnsupportedTransportException("Node IPC exists only in Node.js processes started with an IPC channel")
