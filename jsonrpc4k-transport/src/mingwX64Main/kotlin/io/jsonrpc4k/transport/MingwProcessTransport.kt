package io.jsonrpc4k.transport

import kotlin.time.Duration

/** Child processes are not implemented for Windows native; [ProcessTransport.start] always fails. */
internal actual suspend fun startProcessTransport(
    command: List<String>,
    environment: Map<String, String?>,
    workingDirectory: String?,
    stderr: StderrMode,
    exitTimeout: Duration,
): ProcessTransport = throw UnsupportedTransportException("Process transports are not supported on Windows native")
