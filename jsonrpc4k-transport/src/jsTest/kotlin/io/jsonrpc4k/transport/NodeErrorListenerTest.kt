package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.resume
import kotlin.coroutines.suspendCoroutine
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val SOME_BYTES = byteArrayOf(1, 2, 3)

/** Connects a raw Node socket to [listener] and wraps it, keeping the raw socket so a test can inject events. */
private suspend fun connectRaw(listener: TransportListener): Triple<dynamic, SocketTransport, Transport> {
    val raw: dynamic = Net.createConnection(listener.port, "127.0.0.1") {}
    val peer = listener.accept()
    return Triple(raw, SocketTransport(raw), peer)
}

private fun emitError(
    emitter: dynamic,
    message: String,
) {
    val error: dynamic = js("new Error()")
    error.message = message
    emitter.emit("error", error)
}

// runTest skips delays in virtual time; real sockets need the real clock, hence Dispatchers.Default.
class NodeErrorListenerTest {
    @Test
    fun `error event with nobody collecting incoming does not throw and fails the next send`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val (raw, transport, peer) = connectRaw(listener)
                    try {
                        emitError(raw, "boom before collection")
                        assertFalse(transport.isConnected)
                        val failure = assertFailsWith<TransportException> { transport.send(SOME_BYTES) }
                        assertTrue(failure.message!!.contains("boom before collection"), "failure was $failure")
                    } finally {
                        transport.close()
                        peer.close()
                    }
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `error events after incoming completed and repeated errors are swallowed`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val (raw, transport, peer) = connectRaw(listener)
                    try {
                        val collected = async { transport.incoming.toList() }
                        peer.close()
                        assertTrue(withTimeout(10.seconds) { collected.await() }.isEmpty())
                        emitError(raw, "first after completion")
                        emitError(raw, "second after completion")
                        val failure = assertFailsWith<TransportException> { transport.send(SOME_BYTES) }
                        assertTrue(failure.message!!.contains("first after completion"), "failure was $failure")
                    } finally {
                        transport.close()
                        peer.close()
                    }
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `error event ends an active incoming collection with the failure`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val (raw, transport, peer) = connectRaw(listener)
                    try {
                        val collected = async { runCatching { transport.incoming.toList() } }
                        // Give the collector a turn to attach before the error arrives.
                        delay(50)
                        emitError(raw, "boom while collecting")
                        val failure = withTimeout(10.seconds) { collected.await() }.exceptionOrNull()
                        assertTrue(failure is TransportException, "incoming ended with $failure")
                    } finally {
                        transport.close()
                        peer.close()
                    }
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `error after a deliberate close is swallowed and send reports the closed transport`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                val listener = TcpListener.bind("127.0.0.1", 0)
                try {
                    val (raw, transport, peer) = connectRaw(listener)
                    try {
                        transport.close()
                        emitError(raw, "late error after close")
                        val failure = assertFailsWith<TransportException> { transport.send(SOME_BYTES) }
                        assertTrue(failure.message!!.contains("closed"), "failure was $failure")
                        assertFalse(failure.message!!.contains("late error"), "failure was $failure")
                    } finally {
                        peer.close()
                    }
                } finally {
                    listener.close()
                }
            }
        }

    @Test
    fun `a peer reset while incoming is not collected fails send and the process survives`() =
        runTest(timeout = 30.seconds) {
            withContext(Dispatchers.Default) {
                // A raw Node server so the test can reset the accepted socket from the peer side.
                val server: dynamic = Net.createServer()
                val accepted = CompletableDeferred<dynamic>()
                server.on("connection") { socket: dynamic ->
                    socket.on("error") { _: dynamic -> }
                    accepted.complete(socket)
                }
                suspendCoroutine<Unit> { continuation -> server.listen(0, "127.0.0.1") { continuation.resume(Unit) } }
                try {
                    val port = server.address().port.unsafeCast<Int>()
                    val client = TcpSocket.connect("127.0.0.1", port)
                    val peer = withTimeout(10.seconds) { accepted.await() }
                    if (jsTypeOf(peer.resetAndDestroy) != "function") {
                        // Node before 18.3 cannot send a reset, so there is nothing to provoke.
                        client.close()
                        return@withContext
                    }
                    peer.resetAndDestroy()
                    // The client never collected incoming. The reset must not crash the process, and
                    // once Node reports it, send fails with TransportException.
                    var failure: Throwable? = null
                    repeat(100) {
                        if (failure == null) {
                            failure = runCatching { client.send(SOME_BYTES) }.exceptionOrNull()
                            if (failure == null) delay(50)
                        }
                    }
                    assertTrue(failure is TransportException, "send failed with $failure")
                    client.close()
                } finally {
                    server.close()
                }
            }
        }
}
