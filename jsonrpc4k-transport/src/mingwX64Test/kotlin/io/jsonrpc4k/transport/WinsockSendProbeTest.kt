package io.jsonrpc4k.transport

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import platform.posix.WSAGetLastError
import platform.posix.closesocket
import platform.posix.send
import kotlin.test.Test
import kotlin.test.assertEquals

private const val PROBE_BYTES = 64 * 1024 * 1024

// Temporary probe for PR #77: records what one non-blocking 64 MiB send() to a peer that never reads returns on
// windows-latest, to document why MAX_SEND_BYTES exists. Removed once the outcome is recorded.
class WinsockSendProbeTest {
    @OptIn(ExperimentalForeignApi::class)
    @Test
    fun `one non blocking 64 MiB send to a peer that does not read`(): Unit =
        runBlocking(Dispatchers.Default) {
            val listener = TcpListener.bind("127.0.0.1", 0)
            val sender = connectProbeSocket(listener.port)
            val receiver = listener.accept()
            try {
                check(setNonBlocking(sender)) { "setNonBlocking failed" }
                val payload = ByteArray(PROBE_BYTES)
                val count = payload.usePinned { send(sender, it.addressOf(0), PROBE_BYTES, 0) }
                val error = if (count == SOCKET_ERROR_VALUE) WSAGetLastError() else 0
                assertEquals(PROBE_BYTES, count, "PROBE send returned $count, WSAGetLastError $error")
            } finally {
                receiver.close()
                closesocket(sender)
                listener.close()
            }
        }
}
