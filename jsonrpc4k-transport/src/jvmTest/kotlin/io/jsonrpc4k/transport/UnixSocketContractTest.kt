package io.jsonrpc4k.transport

import java.nio.file.Files
import java.nio.file.Path

class UnixSocketContractTest : TransportContractTest() {
    override suspend fun createPair(): Pair<Transport, Transport> {
        val dir: Path = Files.createTempDirectory("jrpc")
        val path = dir.resolve("c.sock").toString()
        val listener = UnixSocket.bind(path)
        try {
            val client = UnixSocket.connect(path)
            val server = listener.accept()
            return client to server
        } finally {
            listener.close()
            Files.deleteIfExists(dir)
        }
    }
}
