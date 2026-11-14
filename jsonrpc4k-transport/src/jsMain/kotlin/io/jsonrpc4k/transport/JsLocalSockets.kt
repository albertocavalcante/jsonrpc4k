package io.jsonrpc4k.transport

import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.coroutines.suspendCoroutine

private const val ECONNREFUSED = "ECONNREFUSED"

internal fun pathOptions(path: String): dynamic {
    val options: dynamic = js("({})")
    options.path = path
    return options
}

private fun isWindows(): Boolean = js("process.platform === 'win32'").unsafeCast<Boolean>()

private fun requireNode(kind: String) {
    if (!isNodeRuntime()) throw UnsupportedTransportException("$kind require Node.js; browsers cannot open them")
}

private fun requireUnixSockets() {
    requireNode("Unix domain sockets")
    if (isWindows()) throw UnsupportedTransportException("Node.js on Windows serves pipes, not Unix sockets; use NamedPipe")
}

private fun requireNamedPipes() {
    requireNode("Named pipes")
    if (!isWindows()) throw UnsupportedTransportException("Named pipes exist only on Windows; use UnixSocket here")
}

private suspend fun connectPath(path: String): Transport =
    connectNodeSocket(path) { onConnect -> Net.raw().createConnection(pathOptions(path), onConnect) }

internal actual suspend fun connectTcp(
    host: String,
    port: Int,
): Transport {
    requireNode("TCP sockets")
    return SocketTransport.connect(host, port)
}

internal actual suspend fun connectUnixSocket(path: String): Transport {
    requireUnixSockets()
    return connectPath(path)
}

internal actual suspend fun bindUnixSocket(path: String): LocalListener {
    requireUnixSockets()
    removeStaleSocket(path)
    return NodeLocalListener.bind(path)
}

internal actual suspend fun connectNamedPipe(pipePath: String): Transport {
    requireNamedPipes()
    return connectPath(pipePath)
}

internal actual suspend fun bindNamedPipe(pipePath: String): LocalListener {
    requireNamedPipes()
    return NodeLocalListener.bind(pipePath)
}

/**
 * Deletes [path] only when it is a socket that refuses connections. A regular file or a socket that
 * is still served is left alone and makes the following listen fail.
 */
private suspend fun removeStaleSocket(path: String) {
    val isSocket =
        try {
            Fs.lstatSync(path).isSocket().unsafeCast<Boolean>()
        } catch (ignored: Throwable) {
            false
        }
    if (!isSocket) return
    if (isServed(path)) throw TransportException("Unix socket $path is already in use")
    Fs.unlinkSync(path)
}

private suspend fun isServed(path: String): Boolean =
    suspendCoroutine { continuation ->
        var resolved = false
        val socket: dynamic = Net.raw().createConnection(pathOptions(path))
        socket.once("connect") {
            socket.destroy()
            if (!resolved) {
                resolved = true
                continuation.resume(true)
            }
        }
        socket.on("error") { error: dynamic ->
            if (!resolved) {
                resolved = true
                if (error?.code == ECONNREFUSED) {
                    continuation.resume(false)
                } else {
                    continuation.resumeWithException(
                        TransportException("Cannot probe Unix socket $path: ${error?.message ?: error}"),
                    )
                }
            }
        }
    }
