package io.jsonrpc4k.transport

/** The transport an LSP client asked a server to use, from the vscode-languageclient launch flags. */
public sealed interface LspTransportSpec {
    /** `--stdio` */
    public data object Stdio : LspTransportSpec

    /** `--pipe=<name>`: a Unix socket path or a Windows pipe path/name. */
    public data class Pipe(
        val name: String,
    ) : LspTransportSpec

    /** `--socket=<port>`: a TCP port on the loopback interface. */
    public data class Socket(
        val port: Int,
    ) : LspTransportSpec

    /** `--node-ipc` */
    public data object NodeIpc : LspTransportSpec
}

/**
 * Parsed LSP launch arguments: the transport and the optional `--clientProcessId`.
 */
public data class LspLaunchArguments(
    val transport: LspTransportSpec,
    val clientProcessId: Long?,
) {
    public companion object {
        private const val MAX_PORT = 65_535

        /**
         * Parse the flags vscode-languageclient passes to a language server: `--stdio`,
         * `--pipe=<name>`, `--socket=<port>`, `--node-ipc` and `--clientProcessId=<pid>`.
         *
         * Validation is strict. Unknown or positional arguments, a repeated flag, two different
         * transport flags, a value on a flag that takes none, an empty or malformed value and a
         * missing transport are all rejected, unless [defaultTransport] is given for the missing case.
         *
         * @throws IllegalArgumentException describing the first offending argument.
         */
        public fun parse(
            args: List<String>,
            defaultTransport: LspTransportSpec? = null,
        ): LspLaunchArguments {
            var transport: LspTransportSpec? = null
            var clientProcessId: Long? = null
            for (arg in args) {
                val name = arg.substringBefore('=')
                val value = if ('=' in arg) arg.substringAfter('=') else null
                if (name == "--clientProcessId") {
                    require(clientProcessId == null) { "Repeated argument: $name" }
                    clientProcessId = parseProcessId(value)
                } else {
                    val spec = parseTransport(name, value, arg)
                    require(transport == null) { "Conflicting transport arguments: $transport and $spec" }
                    transport = spec
                }
            }
            val resolved =
                transport ?: defaultTransport
                    ?: throw IllegalArgumentException(
                        "Missing transport argument: one of --stdio, --pipe=<name>, --socket=<port>, --node-ipc",
                    )
            return LspLaunchArguments(resolved, clientProcessId)
        }

        private fun parseProcessId(value: String?): Long {
            val pid = value?.takeIf(::isDecimal)?.toLongOrNull()
            require(pid != null && pid > 0) { "--clientProcessId needs a positive integer: --clientProcessId=<pid>" }
            return pid
        }

        private fun parseTransport(
            name: String,
            value: String?,
            arg: String,
        ): LspTransportSpec =
            when (name) {
                "--stdio" -> flag(name, value, LspTransportSpec.Stdio)
                "--node-ipc" -> flag(name, value, LspTransportSpec.NodeIpc)
                "--pipe" -> {
                    require(!value.isNullOrEmpty()) { "--pipe needs a name: --pipe=<name>" }
                    LspTransportSpec.Pipe(value)
                }
                "--socket" -> {
                    val port = value?.takeIf(::isDecimal)?.toIntOrNull()
                    require(port != null && port in 1..MAX_PORT) { "--socket needs a port from 1 to $MAX_PORT: --socket=<port>" }
                    LspTransportSpec.Socket(port)
                }
                else -> throw IllegalArgumentException("Unknown argument: $arg")
            }

        private fun flag(
            name: String,
            value: String?,
            spec: LspTransportSpec,
        ): LspTransportSpec {
            require(value == null) { "$name takes no value" }
            return spec
        }

        private fun isDecimal(text: String): Boolean = text.isNotEmpty() && text.all { it in '0'..'9' }
    }
}

private const val LOOPBACK = "127.0.0.1"

/**
 * Open the transport that [spec] describes, as a client of the endpoint the LSP client prepared.
 *
 * - [LspTransportSpec.Stdio]: [TransportFactory.stdio].
 * - [LspTransportSpec.Pipe]: a Windows pipe when the name is a `\\.\pipe\` path, otherwise a Unix socket.
 * - [LspTransportSpec.Socket]: TCP to 127.0.0.1.
 * - [LspTransportSpec.NodeIpc]: [NodeIpc.connect].
 *
 * @throws TransportException if the endpoint cannot be reached.
 * @throws UnsupportedTransportException if the platform lacks that kind, for example NodeIpc on the JVM.
 */
public suspend fun TransportFactory.connect(spec: LspTransportSpec): Transport =
    when (spec) {
        LspTransportSpec.Stdio -> stdio()
        is LspTransportSpec.Pipe ->
            if (isPipePath(spec.name)) NamedPipe.connect(spec.name) else UnixSocket.connect(spec.name)
        is LspTransportSpec.Socket -> TcpSocket.connect(LOOPBACK, spec.port)
        LspTransportSpec.NodeIpc -> NodeIpc.connect()
    }
