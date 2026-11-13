package io.jsonrpc4k.transport

class InMemoryTransportContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> = InMemoryTransport.pair()
}
