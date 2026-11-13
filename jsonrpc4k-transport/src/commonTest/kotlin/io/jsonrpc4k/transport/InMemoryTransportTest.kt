package io.jsonrpc4k.transport

import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class InMemoryTransportTest {
    @Test
    fun `send copies the buffer so later mutation is not observed`() =
        runTest {
            val (a, b) = InMemoryTransport.pair()
            val data = byteArrayOf(1, 2, 3)
            a.send(data)
            data[0] = 99
            assertContentEquals(byteArrayOf(1, 2, 3), b.incoming.first())
        }

    @Test
    fun `rendezvous capacity suspends send until the peer receives`() =
        runTest {
            val (a, b) = InMemoryTransport.pair(Channel.RENDEZVOUS)
            val sent = async { a.send(byteArrayOf(5)) }
            assertContentEquals(byteArrayOf(5), b.incoming.first())
            sent.await()
        }

    @Test
    fun `send fails after the peer closed`() =
        runTest {
            val (a, b) = InMemoryTransport.pair()
            a.send(byteArrayOf(1))
            b.close()
            assertFailsWith<TransportException> { a.send(byteArrayOf(2)) }
            assertTrue(!b.isConnected)
        }
}
