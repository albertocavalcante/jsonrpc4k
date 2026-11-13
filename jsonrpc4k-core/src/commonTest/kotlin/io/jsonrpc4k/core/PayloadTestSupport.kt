package io.jsonrpc4k.core

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ReceiveChannel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonElement
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.seconds

/** The messages of single-message payloads; fails on a batch or an invalid payload. */
internal fun List<Payload>.messages(): List<Message> = map { assertIs<Payload.Single>(it).message }

/** Frames [text] byte for byte, so malformed JSON reaches the decoder unchanged. */
internal fun wireFrame(text: String): ByteArray {
    val body = text.encodeToByteArray()
    return "Content-Length: ${body.size}\r\n\r\n".encodeToByteArray() + body
}

/** The parsed JSON body of one outgoing frame. */
internal fun wireJson(frame: ByteArray): JsonElement = Json.parseToJsonElement(frame.decodeToString().substringAfter("\r\n\r\n"))

/** Collects the outgoing frames of [connection] from a coroutine in this scope. */
internal fun CoroutineScope.outgoingFrames(connection: Connection): ReceiveChannel<ByteArray> {
    val frames = Channel<ByteArray>(Channel.UNLIMITED)
    launch { connection.outgoing.collect { frames.send(it) } }
    return frames
}

/** Waits for the next outgoing frame. */
internal suspend fun ReceiveChannel<ByteArray>.nextFrame(): ByteArray = withTimeout(5.seconds) { receive() }

private val batchPolicy = BoundedDispatchPolicy(maxConcurrentRequests = 4, maxQueuedMessages = 8)

/** Every dispatch mode: legacy, bounded, and bounded with inbound and outbound resource limits. */
internal val dispatchModes: List<(CoroutineScope) -> Connection> =
    listOf<(CoroutineScope) -> Connection>(
        { scope -> Connection(scope = scope) },
        { scope -> Connection(batchPolicy, scope = scope) },
        { scope ->
            Connection(
                batchPolicy,
                InboundResourceLimits(maxFrameBodyBytes = 65_536, maxJsonNesting = 32, maxApplicationBodyBytes = 65_536),
                OutboundResourceLimits(65_536, 65_536, 8, 8, 65_536, 4, 10_000, 32),
                scope,
            )
        },
    )
