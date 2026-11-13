package io.jsonrpc4k.transport

import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * A stand-in for the parent channel of a forked child: an event emitter with `send` and `disconnect`.
 * Running a real forked child is not possible from inside the test process, which was not forked.
 */
private fun fakeIpcProcess(sent: MutableList<String>): dynamic {
    val process: dynamic = js("new (require('events').EventEmitter)()")
    process.connected = true
    process.send = { message: dynamic, callback: dynamic ->
        sent.add(JSON.stringify(message))
        callback(null)
    }
    process.disconnect = {
        process.connected = false
        process.emit("disconnect")
    }
    return process
}

class NodeIpcTransportTest {
    @Test
    fun `IPC messages flow both ways as JSON`() =
        runTest {
            val sent = mutableListOf<String>()
            val process = fakeIpcProcess(sent)
            val transport = NodeIpcTransport(process)
            val received = async { transport.incoming.first() }
            yield()
            process.emit("message", js("({jsonrpc: '2.0', id: 1})"))
            assertEquals("""{"jsonrpc":"2.0","id":1}""", received.await().decodeToString())

            transport.send("""{"jsonrpc":"2.0","method":"ping"}""".encodeToByteArray())
            assertEquals(listOf("""{"jsonrpc":"2.0","method":"ping"}"""), sent)
            assertFailsWith<TransportException> { transport.send("not json".encodeToByteArray()) }
        }

    @Test
    fun `IPC close disconnects the channel and ends incoming`() =
        runTest {
            val process = fakeIpcProcess(mutableListOf())
            val transport = NodeIpcTransport(process)
            assertTrue(transport.isConnected)
            transport.close()
            transport.close()
            assertFalse(transport.isConnected)
            assertEquals(false, process.connected)
            assertTrue(transport.incoming.toList().isEmpty())
            assertFailsWith<TransportException> { transport.send("{}".encodeToByteArray()) }
        }

    @Test
    fun `IPC parent disconnect ends incoming`() =
        runTest {
            val process = fakeIpcProcess(mutableListOf())
            val transport = NodeIpcTransport(process)
            process.disconnect()
            assertFalse(transport.isConnected)
            assertTrue(transport.incoming.toList().isEmpty())
        }
}

private val PAYLOAD = byteArrayOf(1, 2, 3)

class NamedPipeTest {
    @Test
    fun `named pipes are unsupported off Windows`() =
        runTest {
            if (isWindowsHost()) return@runTest
            assertFailsWith<UnsupportedTransportException> { NamedPipe.bind("jsonrpc4k-test") }
            assertFailsWith<UnsupportedTransportException> { NamedPipe.connect("jsonrpc4k-test") }
        }

    // Only a Windows host can create a pipe; the Windows CI job runs this one.
    @Test
    fun `named pipe round trips bytes on Windows`() =
        runTest {
            if (!isWindowsHost()) return@runTest
            val name = "jsonrpc4k-test-${randomSuffix()}"
            val listener = NamedPipe.bind(name)
            try {
                val client = NamedPipe.connect(name)
                val server = listener.accept()
                val received = async { server.incoming.first() }
                client.send(PAYLOAD)
                assertContentEquals(PAYLOAD, received.await())
                client.close()
                server.close()
            } finally {
                listener.close()
            }
        }
}
