package io.jsonrpc4k.transport

class SocketTransportContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> {
        val listener = TcpListener.bind("127.0.0.1", 0)
        try {
            val client = SocketTransport.connect("127.0.0.1", listener.port)
            val server = listener.accept()
            return client to server
        } finally {
            listener.close()
        }
    }
}
