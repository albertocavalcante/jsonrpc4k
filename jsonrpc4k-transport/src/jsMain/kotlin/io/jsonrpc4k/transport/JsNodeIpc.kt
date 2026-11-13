package io.jsonrpc4k.transport

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

internal actual fun connectNodeIpc(): Transport {
    if (!isNodeRuntime()) throw UnsupportedTransportException("Node IPC requires Node.js")
    val process: dynamic = NodeProcess.raw
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
 *
 * The listeners are registered on construction because Node drops messages that arrive while none
 * is attached, such as a request the parent sends right after fork. Messages are queued until
 * [incoming] is collected.
 */
internal class NodeIpcTransport(
    private val process: dynamic,
) : Transport {
    private var connected = process.connected != false
    private val queue = Channel<ByteArray>(Channel.UNLIMITED)

    private val messageCallback: (dynamic) -> Unit = { message: dynamic ->
        val text: dynamic = JSON.stringify(message)
        if (connected && jsTypeOf(text) == "string") queue.trySend(text.unsafeCast<String>().encodeToByteArray())
    }
    private val disconnectCallback: (dynamic) -> Unit = {
        connected = false
        queue.close()
    }

    init {
        if (connected) {
            process.on("message", messageCallback)
            process.once("disconnect", disconnectCallback)
        } else {
            queue.close()
        }
    }

    override val incoming: Flow<ByteArray> = queue.receiveAsFlow()

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
        process.removeListener("message", messageCallback)
        process.removeListener("disconnect", disconnectCallback)
        queue.close()
        if (connected) {
            connected = false
            if (process.connected != false) process.disconnect()
        }
    }

    override val isConnected: Boolean
        get() = connected && process.connected != false
}
