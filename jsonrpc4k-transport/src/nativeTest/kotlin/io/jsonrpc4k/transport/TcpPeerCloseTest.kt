package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private val PROMPT = 5.seconds
private const val CHUNK_BYTES = 64 * 1024

// Bounds the write loop far above what loopback buffers hold, so a write that never fails ends the test by name.
private const val MAX_WRITES = 4096

/**
 * A peer that is gone must end a waiting read or write at once. WSAPoll reports POLLHUP whatever was requested, so a
 * wait that treated it as "not ready yet" without sleeping spun at full CPU; these cases run on every native target.
 */
class TcpPeerCloseTest {
    @Test
    fun `after the peer closes a read ends with EOF promptly`(): Unit =
        withConnectedPair { peer, local ->
            peer.close()
            val chunks = withTimeoutOrNull(PROMPT) { local.incoming.toList() }
            assertNotNull(chunks, "incoming did not end within $PROMPT after the peer closed")
            assertTrue(chunks.isEmpty(), "incoming delivered $chunks")
            assertFalse(local.isConnected)
        }

    @Test
    fun `after the peer closes a write fails promptly`(): Unit =
        withConnectedPair { peer, local ->
            peer.close()
            val outcome =
                withTimeoutOrNull(PROMPT) {
                    runCatching { repeat(MAX_WRITES) { local.send(ByteArray(CHUNK_BYTES)) } }
                }
            assertNotNull(outcome, "writes kept waiting for $PROMPT after the peer closed")
            val failure = outcome.exceptionOrNull()
            assertTrue(failure is TransportException, "writes to a closed peer ended with $failure")
        }

    private fun withConnectedPair(block: suspend (peer: Transport, local: Transport) -> Unit): Unit =
        runBlocking(Dispatchers.Default) {
            withTimeout(TIMEOUT) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val peer = TcpSocket.connect("127.0.0.1", listener.port)
                    val local = listener.accept()
                    try {
                        block(peer, local)
                    } finally {
                        peer.close()
                        local.close()
                    }
                } finally {
                    listener.close()
                }
            }
        }
}
