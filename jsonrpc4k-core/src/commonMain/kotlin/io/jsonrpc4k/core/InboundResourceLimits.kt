package io.jsonrpc4k.core

/**
 * Opt-in limits for framed inbound data. Body bytes are a wire-size proxy, not heap usage.
 * The application budget covers queued, selected, and running requests and notifications.
 * One decoder frame and up to 16 KiB of header buffering, caller-owned input, decoded expansion,
 * outgoing data, and handler-owned allocations are additional. Responses and cancellation bypass
 * the application budget but still obey frame and nesting limits.
 * Direct codec calls and the legacy synchronous connection are not limited by this policy.
 *
 * @param maxFrameBodyBytes Largest inbound Content-Length, from 1 to 100 MiB.
 * @param maxJsonNesting Maximum nesting of JSON objects and arrays, from 1 to 256.
 * @param maxApplicationBodyBytes Maximum combined original body bytes of retained application work,
 * from 1 to 100 MiB. This does not include responses or cancellation notifications.
 */
public data class InboundResourceLimits(
    val maxFrameBodyBytes: Int,
    val maxJsonNesting: Int,
    val maxApplicationBodyBytes: Int,
) {
    init {
        require(maxFrameBodyBytes in 1..JsonRpcCodec.MAX_CONTENT_LENGTH)
        require(maxJsonNesting in 1..MAX_JSON_NESTING)
        require(maxApplicationBodyBytes in 1..JsonRpcCodec.MAX_CONTENT_LENGTH)
    }

    private companion object {
        const val MAX_JSON_NESTING = 256
    }
}
