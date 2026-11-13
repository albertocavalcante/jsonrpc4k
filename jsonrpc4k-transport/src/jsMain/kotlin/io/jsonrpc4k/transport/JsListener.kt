package io.jsonrpc4k.transport

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedReceiveChannelException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal actual suspend fun bindTcpListener(
    host: String,
    port: Int,
): TransportListener {
    if (!isNodeRuntime()) {
        throw UnsupportedTransportException("TCP listeners require Node.js; browsers cannot accept connections")
    }
    return NodeTcpListener.bind(host, port)
}

private fun isNodeRuntime(): Boolean =
    js("typeof process !== 'undefined' && process.versions != null && process.versions.node != null")
        .unsafeCast<Boolean>()

/**
 * TCP listener backed by `net.createServer`. Accepted sockets are queued in a channel until
 * [accept] takes them.
 */
internal class NodeTcpListener private constructor(
    private val server: dynamic,
    private val accepted: Channel<SocketTransport>,
    override val port: Int,
) : TransportListener {
    override suspend fun accept(): Transport =
        try {
            accepted.receive()
        } catch (e: ClosedReceiveChannelException) {
            throw TransportException("Listener is closed", e)
        }

    override suspend fun close() {
        server.close()
        accepted.close()
        // Connections nobody accepted yet are released with the listener.
        while (true) {
            val pending = accepted.tryReceive().getOrNull() ?: break
            pending.close()
        }
    }

    companion object {
        suspend fun bind(
            host: String,
            port: Int,
        ): NodeTcpListener {
            val accepted = Channel<SocketTransport>(Channel.UNLIMITED)
            val server = Net.createServer()
            server.on("connection") { socket: dynamic ->
                accepted.trySend(SocketTransport(socket))
            }
            val boundPort =
                suspendCoroutine<Int> { continuation ->
                    var resolved = false
                    server.on("error") { error: dynamic ->
                        val failure = TransportException("Failed to listen on $host:$port: ${error?.message ?: error}")
                        if (resolved) {
                            accepted.close(failure)
                        } else {
                            resolved = true
                            continuation.resumeWithException(failure)
                        }
                    }
                    server.listen(port, host) {
                        if (!resolved) {
                            resolved = true
                            continuation.resume(server.address().port.unsafeCast<Int>())
                        }
                    }
                }
            return NodeTcpListener(server, accepted, boundPort)
        }
    }
}
