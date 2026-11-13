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

internal fun isNodeRuntime(): Boolean =
    js("typeof process !== 'undefined' && process.versions != null && process.versions.node != null")
        .unsafeCast<Boolean>()

/**
 * Listener backed by a Node.js `net` server. Accepted sockets are queued in a channel until [accept]
 * takes them.
 */
internal abstract class NodeServerListener(
    private val server: dynamic,
    private val accepted: Channel<SocketTransport>,
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
        /**
         * Create a server that queues connections into [accepted], start it with [listen], and wait until
         * it listens. [target] only names the endpoint in failure messages.
         */
        suspend fun start(
            target: String,
            accepted: Channel<SocketTransport>,
            listen: (server: dynamic, onListening: () -> Unit) -> Unit,
        ): dynamic {
            val server = Net.createServer()
            server.on("connection") { socket: dynamic ->
                accepted.trySend(SocketTransport(socket))
            }
            suspendCoroutine<Unit> { continuation ->
                var resolved = false
                server.on("error") { error: dynamic ->
                    val failure = TransportException("Failed to listen on $target: ${error?.message ?: error}")
                    if (resolved) {
                        accepted.close(failure)
                    } else {
                        resolved = true
                        continuation.resumeWithException(failure)
                    }
                }
                listen(server) {
                    if (!resolved) {
                        resolved = true
                        continuation.resume(Unit)
                    }
                }
            }
            return server
        }
    }
}

internal class NodeTcpListener private constructor(
    server: dynamic,
    accepted: Channel<SocketTransport>,
    override val port: Int,
) : NodeServerListener(server, accepted) {
    companion object {
        suspend fun bind(
            host: String,
            port: Int,
        ): NodeTcpListener {
            val accepted = Channel<SocketTransport>(Channel.UNLIMITED)
            val server =
                NodeServerListener.start("$host:$port", accepted) { node, onListening ->
                    node.listen(port, host, onListening)
                }
            return NodeTcpListener(server, accepted, server.address().port.unsafeCast<Int>())
        }
    }
}

/** Unix socket or named pipe listener; Node deletes a Unix socket file when the server closes. */
internal class NodeLocalListener private constructor(
    server: dynamic,
    accepted: Channel<SocketTransport>,
    override val path: String,
) : NodeServerListener(server, accepted),
    LocalListener {
    override val port: Int
        get() = -1

    companion object {
        suspend fun bind(path: String): NodeLocalListener {
            val accepted = Channel<SocketTransport>(Channel.UNLIMITED)
            val server =
                NodeServerListener.start(path, accepted) { node, onListening ->
                    node.listen(pathOptions(path), onListening)
                }
            return NodeLocalListener(server, accepted, path)
        }
    }
}
