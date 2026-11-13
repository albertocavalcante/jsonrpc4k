package io.jsonrpc4k.transport

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow

/**
 * One end of an in-process, channel-backed transport pair created by [pair].
 *
 * Bytes sent on one end are delivered, in order, to [incoming] of the other end. Closing either end
 * completes the peer's [incoming] normally after the bytes already delivered to it have been read,
 * and makes the peer's [send] throw [TransportException]. Intended for tests and for wiring a client
 * and a server in the same process.
 */
public class InMemoryTransport private constructor(
    private val receiving: Channel<ByteArray>,
    private val sending: Channel<ByteArray>,
) : Transport {
    override val incoming: Flow<ByteArray> = receiving.receiveAsFlow()

    override suspend fun send(data: ByteArray) {
        try {
            sending.send(data.copyOf())
        } catch (e: ClosedSendChannelException) {
            throw TransportException("Transport is closed", e)
        }
    }

    override suspend fun close() {
        // EOF for the peer once it drained what was already sent.
        sending.close()
        // Reject further sends from the peer, then drop anything still queued for us so that a
        // pending collection of [incoming] ends right away.
        receiving.close()
        while (receiving.tryReceive().isSuccess) {
            // Discarding queued chunks.
        }
    }

    @OptIn(DelicateCoroutinesApi::class)
    override val isConnected: Boolean
        get() = !sending.isClosedForSend && !receiving.isClosedForReceive

    public companion object {
        /**
         * Create two connected transports.
         *
         * @param capacity buffer size, in chunks, of each direction; sends suspend while the buffer is
         * full. Accepts the same values as [Channel], including [Channel.UNLIMITED].
         */
        public fun pair(capacity: Int = Channel.BUFFERED): Pair<InMemoryTransport, InMemoryTransport> {
            val aToB = Channel<ByteArray>(capacity)
            val bToA = Channel<ByteArray>(capacity)
            return InMemoryTransport(receiving = bToA, sending = aToB) to
                InMemoryTransport(receiving = aToB, sending = bToA)
        }
    }
}
