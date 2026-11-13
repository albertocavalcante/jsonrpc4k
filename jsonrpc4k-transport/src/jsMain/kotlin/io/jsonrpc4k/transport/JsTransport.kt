package io.jsonrpc4k.transport

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import org.khronos.webgl.Int8Array
import org.khronos.webgl.Uint8Array
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

/**
 * Loads a Node.js built-in module on first use.
 *
 * The modules are deliberately not declared with a module annotation: that form makes the Kotlin/JS
 * output import them when the file loads, which breaks a browser bundle that only uses the core APIs.
 * Resolving them here, behind a runtime check and through a call a bundler cannot follow statically,
 * keeps the import lazy. It relies on the default Kotlin/JS module kind (UMD), where `require` is in scope.
 */
internal fun nodeRequire(name: String): dynamic {
    if (!isNodeRuntime()) {
        throw UnsupportedTransportException("The '$name' module requires Node.js; it is not available in a browser")
    }
    val require: dynamic = js("require")
    return require(name)
}

/** The Node.js process object, resolved on first access. */
internal object NodeProcess {
    val raw: dynamic get() = nodeRequire("process")
    val stdin: dynamic get() = raw.stdin
    val stdout: dynamic get() = raw.stdout
    val env: dynamic get() = raw.env
    val execPath: String get() = raw.execPath.unsafeCast<String>()
}

/** The Node.js fs module, resolved on first access. */
internal object Fs {
    fun lstatSync(path: String): dynamic = nodeRequire("fs").lstatSync(path)

    fun unlinkSync(path: String) {
        nodeRequire("fs").unlinkSync(path)
    }
}

/** The Node.js buffer module, resolved on first access. */
internal object BufferModule {
    val Buffer: dynamic get() = nodeRequire("buffer").Buffer
}

/** The Node.js net module, resolved on first access. */
internal object Net {
    fun raw(): dynamic = nodeRequire("net")

    fun createConnection(
        port: Int,
        host: String,
        callback: () -> Unit,
    ): dynamic = nodeRequire("net").createConnection(port, host, callback)

    fun createServer(): dynamic = nodeRequire("net").createServer()
}

/**
 * Stdio transport for JS/Node.js.
 * Uses process.stdin for reading and process.stdout for writing.
 */
public class StdioTransport : Transport {
    private var connected = true
    private val stdin: dynamic = NodeProcess.stdin
    private val stdout: dynamic = NodeProcess.stdout

    override val incoming: Flow<ByteArray> =
        callbackFlow {
            val dataCallback: (dynamic) -> Unit = { data: dynamic ->
                if (connected) {
                    // No setEncoding: Node delivers raw Buffer chunks, never strings.
                    val bytes = bufferToByteArray(data.unsafeCast<Uint8Array>())
                    trySend(bytes)
                }
            }

            val endCallback: (dynamic) -> Unit = {
                connected = false
                close()
            }

            val errorCallback: (dynamic) -> Unit = { error: dynamic ->
                connected = false
                close(TransportException("Stream error: ${error?.message ?: error}"))
            }

            stdin.on("data", dataCallback)
            stdin.once("end", endCallback)
            stdin.once("error", errorCallback)
            stdin.resume()

            awaitClose {
                stdin.removeListener("data", dataCallback)
                stdin.removeListener("end", endCallback)
                stdin.removeListener("error", errorCallback)
            }
        }.buffer(Channel.UNLIMITED)

    override suspend fun send(data: ByteArray) {
        if (!connected) {
            throw TransportException("Transport is closed")
        }
        suspendCoroutine { continuation ->
            val buffer = byteArrayToBuffer(data)
            stdout.write(buffer) { error: dynamic ->
                if (error != null && error != undefined) {
                    continuation.resumeWithException(
                        TransportException("Write failed: ${error.message ?: error}"),
                    )
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    override suspend fun close() {
        if (connected) {
            connected = false
            stdin.destroy()
            stdout.end()
        }
    }

    override val isConnected: Boolean
        get() = connected
}

/**
 * Socket transport for JS/Node.js.
 */
public class SocketTransport internal constructor(
    private val socket: dynamic,
) : Transport {
    private var connected = true

    override val incoming: Flow<ByteArray> =
        callbackFlow {
            val dataCallback: (dynamic) -> Unit = { data: dynamic ->
                if (connected) {
                    // No setEncoding: Node delivers raw Buffer chunks, never strings.
                    val bytes = bufferToByteArray(data.unsafeCast<Uint8Array>())
                    trySend(bytes)
                }
            }

            val endCallback: (dynamic) -> Unit = {
                connected = false
                close()
            }

            val errorCallback: (dynamic) -> Unit = { error: dynamic ->
                connected = false
                close(TransportException("Socket error: ${error?.message ?: error}"))
            }

            socket.on("data", dataCallback)
            socket.once("end", endCallback)
            socket.once("close", endCallback)
            socket.once("error", errorCallback)

            awaitClose {
                socket.removeListener("data", dataCallback)
                socket.removeListener("end", endCallback)
                socket.removeListener("close", endCallback)
                socket.removeListener("error", errorCallback)
            }
        }.buffer(Channel.UNLIMITED)

    override suspend fun send(data: ByteArray) {
        if (!connected) {
            throw TransportException("Transport is closed")
        }
        suspendCoroutine { continuation ->
            val buffer = byteArrayToBuffer(data)
            socket.write(buffer) { error: dynamic ->
                if (error != null && error != undefined) {
                    continuation.resumeWithException(
                        TransportException("Write failed: ${error.message ?: error}"),
                    )
                } else {
                    continuation.resume(Unit)
                }
            }
        }
    }

    override suspend fun close() {
        if (connected) {
            connected = false
            socket.destroy()
        }
    }

    override val isConnected: Boolean
        get() = connected && !(socket.destroyed as Boolean)

    public companion object {
        /**
         * Connect to a socket server.
         */
        public suspend fun connect(
            host: String,
            port: Int,
        ): SocketTransport = connectNodeSocket("$host:$port") { onConnect -> Net.createConnection(port, host, onConnect) }
    }
}

/**
 * Create a Node.js socket with [create], which receives the connect callback, and wait for it to connect.
 * [target] only names the endpoint in the failure message.
 */
internal suspend fun connectNodeSocket(
    target: String,
    create: (onConnect: () -> Unit) -> dynamic,
): SocketTransport =
    suspendCoroutine { continuation ->
        var resolved = false
        var socketRef: dynamic = null

        socketRef =
            create {
                if (!resolved) {
                    resolved = true
                    continuation.resume(SocketTransport(socketRef))
                }
            }

        socketRef.once("error") { error: dynamic ->
            if (!resolved) {
                resolved = true
                continuation.resumeWithException(
                    TransportException("Failed to connect to $target: ${error?.message ?: error}"),
                )
            }
        }
    }

/**
 * Copy the bytes of a Node.js Buffer chunk (a Uint8Array) into a new ByteArray.
 *
 * The chunk may be a slice of a shared, pooled ArrayBuffer, so the view honours its byteOffset and
 * the result is a fresh copy that never aliases Node's memory. A Kotlin/JS ByteArray is an Int8Array.
 */
internal fun bufferToByteArray(chunk: Uint8Array): ByteArray {
    val view = Int8Array(chunk.buffer, chunk.byteOffset, chunk.length)
    return Int8Array(view).unsafeCast<ByteArray>()
}

/**
 * Copy a Kotlin ByteArray into a new Node.js Buffer holding the same bytes (0..255).
 */
internal fun byteArrayToBuffer(bytes: ByteArray): dynamic {
    val view = bytes.unsafeCast<Int8Array>()
    return BufferModule.Buffer.from(Uint8Array(view.buffer, view.byteOffset, view.length))
}
