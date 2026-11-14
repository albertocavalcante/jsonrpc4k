package io.jsonrpc4k.transport

import kotlin.js.Date
import kotlin.test.Test
import kotlin.test.assertFailsWith

private fun tempSocketPath(): String {
    val dir: String = js("process.env.TMPDIR || '/tmp'").unsafeCast<String>()
    return "$dir/jrpc-${Date.now().toLong()}-${(js("Math.random()").unsafeCast<Double>() * 1e9).toLong()}.sock"
}

/** Runs the shared contract over Node Unix domain sockets (Node on POSIX; Windows has no Unix sockets). */
class UnixSocketContractTest : TransportContractTest() {
    override fun isSupportedHere(): Boolean = !isWindowsHost()

    override suspend fun createPair(): Pair<Transport, Transport> {
        val path = tempSocketPath()
        val listener = UnixSocket.bind(path)
        try {
            val client = UnixSocket.connect(path)
            val server = listener.accept()
            return client to server
        } finally {
            listener.close()
        }
    }
}

class NodeIpcTest {
    @Test
    fun `Node IPC without a channel is unsupported`() {
        if (!isNodeRuntime()) return
        if (jsTypeOf(js("process.send")) == "function") return
        assertFailsWith<UnsupportedTransportException> { NodeIpc.connect() }
    }
}
