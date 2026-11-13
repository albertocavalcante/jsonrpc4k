package io.jsonrpc4k.transport

/** Runs the shared contract over native TCP loopback sockets (POSIX and Windows). */
class TcpTransportContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> {
        val listener = TcpListener.bind("127.0.0.1", 0)
        try {
            val client = TcpSocket.connect("127.0.0.1", listener.port)
            val server = listener.accept()
            return client to server
        } finally {
            listener.close()
        }
    }
}
