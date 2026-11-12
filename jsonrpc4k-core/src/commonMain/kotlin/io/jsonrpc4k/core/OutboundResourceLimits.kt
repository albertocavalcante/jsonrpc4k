package io.jsonrpc4k.core

/** Limits for the opt-in fixed-default-codec outbound path. Bytes include frame headers.
 *
 * Outstanding frames include the frame currently inside the direct collector. At most one
 * ordinary and one control encoder run concurrently; their temporary allocations, caller-owned
 * objects, typed conversion and downstream buffering are outside the retained-byte budget.
 * A direct collector must not await a reentrant send on this connection: its own frame can
 * hold the capacity needed by that send. Flush markers consume ordinary outstanding slots.
 * Ordinary producers are admitted in mutex-acquisition order. Control frames can bypass a
 * waiting ordinary producer or flush; a flush covers frames already accepted before its marker,
 * not concurrent sends that have not yet been accepted. At most [maxPendingProducers] ordinary
 * producers may wait for admission; additional callers fail immediately. A concurrent control
 * encoder or exhausted control reserve terminates the connection rather than waiting.
 */
public data class OutboundResourceLimits(
    val maxFrameWireBytes: Int,
    val maxRetainedWireBytes: Int,
    val maxOutstandingFrames: Int,
    val maxPendingProducers: Int,
    val controlReserveBytes: Int,
    val controlReserveFrames: Int,
    val maxNodes: Int,
    val maxJsonNesting: Int,
) {
    init {
        require(maxFrameWireBytes in 1..JsonRpcCodec.MAX_CONTENT_LENGTH)
        require(maxRetainedWireBytes in maxFrameWireBytes..JsonRpcCodec.MAX_CONTENT_LENGTH)
        require(maxOutstandingFrames in 1..MAX_COUNT)
        require(maxPendingProducers in 1..MAX_COUNT)
        require(controlReserveBytes in 1..JsonRpcCodec.MAX_CONTENT_LENGTH)
        require(controlReserveFrames in 1..MAX_COUNT)
        require(maxNodes in 1..MAX_NODES)
        require(maxJsonNesting in 1..MAX_DEPTH)
    }

    private companion object {
        const val MAX_COUNT = 1024
        const val MAX_NODES = 1_000_000
        const val MAX_DEPTH = 256
    }
}
