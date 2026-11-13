package io.jsonrpc4k.transport

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal actual fun connectNodeIpc(): Transport {
    if (!isNodeRuntime()) throw UnsupportedTransportException("Node IPC requires Node.js")
    val process: dynamic = NodeProcess.asDynamic()
    if (jsTypeOf(process.send) != "function") {
        throw UnsupportedTransportException(
            "This Node.js process has no IPC channel; start it with child_process.fork or an 'ipc' stdio entry",
        )
    }
    return NodeIpcTransport(process)
}

/**
 * Transport over the Node.js IPC channel. Node serializes each message itself, so the transport
 * converts between the byte payloads of [Transport] and one JSON value per IPC message.
 */
internal class NodeIpcTransport(
    private val process: dynamic,
) : Transport {
    private var connected = true

    override val incoming: Flow<ByteArray> =
        callbackFlow {
            val messageCallback: (dynamic) -> Unit = { message: dynamic ->
                if (connected) trySend(JSON.stringify(message).encodeToByteArray())
            }
            val disconnectCallback: (dynamic) -> Unit = {
                connected = false
                close()
            }

            process.on("message", messageCallback)
            process.once("disconnect", disconnectCallback)

            awaitClose {
                process.removeListener("message", messageCallback)
                process.removeListener("disconnect", disconnectCallback)
            }
        }.buffer(Channel.UNLIMITED)

    override suspend fun send(data: ByteArray) {
        if (!connected || process.connected == false) throw TransportException("Transport is closed")
        val message: dynamic =
            try {
                JSON.parse<dynamic>(data.decodeToString(throwOnInvalidSequence = true))
            } catch (e: Throwable) {
                throw TransportException("Node IPC payload is not one UTF-8 JSON value", e)
            }
        suspendCoroutine { continuation ->
            process.send(message) { error: dynamic ->
                if (error != null && error != undefined) {
                    continuation.resumeWithException(TransportException("Write failed: ${error.message ?: error}"))
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    override suspend fun close() {
        if (connected) {
            connected = false
            if (process.connected != false) process.disconnect()
        }
    }

    override val isConnected: Boolean
        get() = connected && process.connected != false
}
