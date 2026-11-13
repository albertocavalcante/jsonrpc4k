package io.jsonrpc4k.transport

private const val WINDOWS_PIPE_PREFIX = "\\\\.\\pipe\\"
private const val FORWARD_PIPE_PREFIX = "//./pipe/"

/**
 * Connect to a TCP server. Suspends until the connection is established.
 *
 * Available on JVM, Node.js, POSIX native and Windows native; throws [UnsupportedTransportException]
 * in a browser.
 */
public object TcpSocket {
    /**
     * @throws TransportException if the host cannot be resolved or the connection fails.
     */
    public suspend fun connect(
        host: String,
        port: Int,
    ): Transport = connectTcp(host, port)
}

/**
 * Unix domain sockets: `UnixSocket.connect("/tmp/server.sock")`.
 *
 * | Platform | Support |
 * |---|---|
 * | JVM (JDK 16+) | connect and bind |
 * | Node.js (not Windows) | connect and bind |
 * | Linux and Apple native | connect and bind |
 * | Windows native, browser | [UnsupportedTransportException] |
 *
 * The path must fit the operating system's `sockaddr_un` limit (about 104 bytes on Apple, 108 on
 * Linux, including the terminator); a longer path fails with [TransportException].
 */
public object UnixSocket {
    /**
     * @throws TransportException if the path is too long or nothing is listening there.
     * @throws UnsupportedTransportException on platforms without Unix domain sockets.
     */
    public suspend fun connect(path: String): Transport = connectUnixSocket(path)

    /**
     * Listen on [path]. A stale socket file (a socket nobody listens on) is replaced. A socket that
     * is still served, or a path that is not a socket, makes the call fail with [TransportException]
     * and leaves the file untouched. Closing the listener deletes the socket file.
     *
     * @throws TransportException if the path is too long, in use or cannot be bound.
     * @throws UnsupportedTransportException on platforms without Unix domain sockets.
     */
    public suspend fun bind(path: String): LocalListener = bindUnixSocket(path)
}

/**
 * Windows named pipes. The argument is a bare pipe name (`jsonrpc`) or a full pipe path
 * (`\\.\pipe\jsonrpc`).
 *
 * | Platform | Support |
 * |---|---|
 * | Node.js on Windows | connect and bind |
 * | JVM, POSIX native, Windows native, browser | [UnsupportedTransportException] |
 *
 * Windows native is unsupported until a Windows runner can validate it.
 */
public object NamedPipe {
    /**
     * Connect to a pipe server.
     *
     * The JVM is unsupported on purpose: a single synchronous handle cannot read and write at once
     * on Windows, which would deadlock request/response traffic.
     *
     * @throws TransportException if the name is invalid or no pipe server is available.
     * @throws UnsupportedTransportException on platforms without client support.
     */
    public suspend fun connect(nameOrPath: String): Transport = connectNamedPipe(pipePath(nameOrPath))

    /**
     * Create a pipe server.
     *
     * @throws TransportException if the name is invalid or the pipe cannot be created.
     * @throws UnsupportedTransportException on platforms without server support.
     */
    public suspend fun bind(nameOrPath: String): LocalListener = bindNamedPipe(pipePath(nameOrPath))
}

/**
 * Node.js IPC channel to the parent process, present when the process was started with
 * `child_process.fork` or an equivalent `ipc` stdio entry (`--node-ipc` in LSP).
 */
public object NodeIpc {
    /**
     * The transport over `process.send` and `process.on("message")`.
     *
     * The channel is message based, not a byte stream: every [Transport.send] payload must be one
     * complete UTF-8 JSON value, and every chunk of [Transport.incoming] is one message serialized
     * as JSON. No `Content-Length` framing is used.
     *
     * @throws UnsupportedTransportException when there is no IPC channel, and on every platform
     * other than Node.js.
     */
    public fun connect(): Transport = connectNodeIpc()
}

/** Whether [name] already is a Windows pipe path rather than a bare pipe name. */
internal fun isPipePath(name: String): Boolean = listOf(WINDOWS_PIPE_PREFIX, FORWARD_PIPE_PREFIX).any(name::startsWith)

private fun isForbiddenPipeNameChar(char: Char): Boolean = char == '\\' || char == '/' || char == '\u0000'

/** Normalizes a pipe name or either pipe path spelling to `\\.\pipe\name`. */
internal fun pipePath(nameOrPath: String): String {
    val name =
        when {
            nameOrPath.startsWith(WINDOWS_PIPE_PREFIX) -> nameOrPath.removePrefix(WINDOWS_PIPE_PREFIX)
            nameOrPath.startsWith(FORWARD_PIPE_PREFIX) -> nameOrPath.removePrefix(FORWARD_PIPE_PREFIX)
            else -> nameOrPath
        }
    if (name.isEmpty() || name.any(::isForbiddenPipeNameChar)) {
        throw TransportException("Invalid named pipe name: '$nameOrPath'")
    }
    return WINDOWS_PIPE_PREFIX + name
}

internal expect suspend fun connectTcp(
    host: String,
    port: Int,
): Transport

internal expect suspend fun connectUnixSocket(path: String): Transport

internal expect suspend fun bindUnixSocket(path: String): LocalListener

/** [pipePath] is already normalized by [pipePath]. */
internal expect suspend fun connectNamedPipe(pipePath: String): Transport

internal expect suspend fun bindNamedPipe(pipePath: String): LocalListener

internal expect fun connectNodeIpc(): Transport
