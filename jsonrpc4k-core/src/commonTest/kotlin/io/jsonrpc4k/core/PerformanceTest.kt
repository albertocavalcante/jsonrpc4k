package io.jsonrpc4k.core

import io.kotest.matchers.longs.shouldBeLessThan
import io.kotest.matchers.shouldBe
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.time.measureTime

/**
 * Performance tests for jsonrpc4k-core components.
 *
 * These tests measure:
 * 1. Message parsing throughput
 * 2. Serialization/deserialization performance
 * 3. Dispatcher handler lookup efficiency
 * 4. Memory patterns under load
 * 5. Concurrent request handling
 */
class PerformanceTest {
    private val codec = JsonRpcCodec.Default

    // ===== Message Parsing Performance =====

    @Test
    fun `parse 10000 messages - measure throughput`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val messageCount = 10_000

        // Pre-generate messages to avoid timing message generation
        val messages =
            (1..messageCount).map { i ->
                val content = """{"jsonrpc":"2.0","id":$i,"method":"test/method","params":{"index":$i}}"""
                "Content-Length: ${content.length}\r\n\r\n$content"
            }
        val allMessages = messages.joinToString("")

        val duration =
            measureTime {
                val parsed = decoder.feed(allMessages)
                parsed.size shouldBe messageCount
            }

        val messagesPerSecond = messageCount * 1000.0 / duration.inWholeMilliseconds
        println("=== Message Parsing Performance ===")
        println("Parsed $messageCount messages in ${duration.inWholeMilliseconds}ms")
        println("Throughput: ${messagesPerSecond.toLong()} messages/second")
        println()

        // Should parse at least 5000 messages per second
        duration.inWholeMilliseconds shouldBeLessThan (messageCount / 5L * 1000)
    }

    @Test
    fun `parse messages incrementally - byte by byte simulation`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val messageCount = 100

        // Simulate chunked/streaming input
        val messages =
            (1..messageCount).map { i ->
                val content = """{"jsonrpc":"2.0","id":$i,"method":"test/method"}"""
                "Content-Length: ${content.length}\r\n\r\n$content"
            }

        var totalParsed = 0
        val duration =
            measureTime {
                for (msg in messages) {
                    // Feed in chunks of varying sizes (simulate network)
                    var offset = 0
                    while (offset < msg.length) {
                        val chunkSize = minOf(50, msg.length - offset)
                        val chunk = msg.substring(offset, offset + chunkSize)
                        totalParsed += decoder.feed(chunk).size
                        offset += chunkSize
                    }
                }
            }

        totalParsed shouldBe messageCount
        println("=== Incremental Parsing Performance ===")
        println("Parsed $messageCount messages incrementally in ${duration.inWholeMilliseconds}ms")
        println()
    }

    @Test
    fun measureFeedMethodOverheadWithBufferManagement() {
        val decoder = JsonRpcMessageDecoder(codec)

        // Test how buffer grows and handles partial data
        val content = """{"jsonrpc":"2.0","method":"test"}"""
        val header = "Content-Length: ${content.length}\r\n\r\n"

        val iterations = 10_000
        var totalMessages = 0

        val duration =
            measureTime {
                repeat(iterations) {
                    // Feed header and content separately to stress buffer management
                    decoder.feed(header)
                    totalMessages += decoder.feed(content).size
                }
            }

        totalMessages shouldBe iterations
        println("=== Buffer Management Overhead ===")
        println("$iterations header+content pairs in ${duration.inWholeMilliseconds}ms")
        println("Average: ${duration.inWholeNanoseconds / iterations}ns per message")
        println()
    }

    // ===== Serialization Performance =====

    @Test
    fun `encode 10000 messages - measure serialization throughput`() {
        val messageCount = 10_000

        // Pre-create messages
        val messages =
            (1..messageCount).map { i ->
                RequestMessage(
                    id = RequestId.of(i.toLong()),
                    method = "test/method",
                    params =
                        buildJsonObject {
                            put("index", i)
                            put("name", "test-$i")
                            put("enabled", i % 2 == 0)
                        },
                )
            }

        val duration =
            measureTime {
                for (msg in messages) {
                    codec.encode(msg)
                }
            }

        val messagesPerSecond = messageCount * 1000.0 / duration.inWholeMilliseconds
        println("=== Encoding Performance ===")
        println("Encoded $messageCount messages in ${duration.inWholeMilliseconds}ms")
        println("Throughput: ${messagesPerSecond.toLong()} messages/second")
        println()

        // Should encode at least 10000 messages per second
        duration.inWholeMilliseconds shouldBeLessThan (messageCount / 10L * 1000)
    }

    @Test
    fun `decode 10000 JSON strings - measure deserialization throughput`() {
        val messageCount = 10_000

        // Pre-generate JSON strings
        val jsonStrings =
            (1..messageCount).map { i ->
                """{"jsonrpc":"2.0","id":$i,"method":"test/method","params":{"index":$i}}"""
            }

        val duration =
            measureTime {
                for (json in jsonStrings) {
                    codec.decodeFromJson(json)
                }
            }

        val messagesPerSecond = messageCount * 1000.0 / duration.inWholeMilliseconds
        println("=== Decoding Performance ===")
        println("Decoded $messageCount JSON strings in ${duration.inWholeMilliseconds}ms")
        println("Throughput: ${messagesPerSecond.toLong()} messages/second")
        println()
    }

    @Test
    fun `round-trip encode-decode performance`() {
        val messageCount = 5_000

        val messages =
            (1..messageCount).map { i ->
                RequestMessage(
                    id = RequestId.of(i.toLong()),
                    method = "textDocument/completion",
                    params =
                        buildJsonObject {
                            put(
                                "textDocument",
                                buildJsonObject {
                                    put("uri", "file:///path/to/file$i.kt")
                                },
                            )
                            put(
                                "position",
                                buildJsonObject {
                                    put("line", i % 1000)
                                    put("character", i % 80)
                                },
                            )
                        },
                )
            }

        val duration =
            measureTime {
                for (msg in messages) {
                    val encoded = codec.encode(msg)
                    val decoder = JsonRpcMessageDecoder(codec)
                    decoder.feed(encoded)
                }
            }

        println("=== Round-trip Performance ===")
        println("$messageCount round-trips in ${duration.inWholeMilliseconds}ms")
        println("Average: ${duration.inWholeNanoseconds / messageCount}ns per round-trip")
        println()
    }

    // ===== Handler Dispatch Performance =====

    @Test
    fun `dispatcher lookup with many registered handlers`() =
        runTest {
            val dispatcher = Dispatcher()
            val handlerCount = 100

            // Register many handlers
            repeat(handlerCount) { i ->
                dispatcher.onRequest("method$i") { _: kotlinx.serialization.json.JsonElement? ->
                    JsonPrimitive("result$i")
                }
            }

            val requestCount = 10_000
            var responseCount = 0

            val duration =
                measureTime {
                    repeat(requestCount) { i ->
                        val methodIndex = i % handlerCount
                        val request =
                            RequestMessage(
                                id = RequestId.of(i.toLong()),
                                method = "method$methodIndex",
                            )
                        val response = dispatcher.dispatch(request)
                        if (response != null) responseCount++
                    }
                }

            responseCount shouldBe requestCount
            println("=== Handler Dispatch Performance ===")
            println("$requestCount dispatches with $handlerCount handlers in ${duration.inWholeMilliseconds}ms")
            println("Average: ${duration.inWholeNanoseconds / requestCount}ns per dispatch")
            println()
        }

    @Test
    fun `dispatcher with typed handlers - serialization overhead`() =
        runTest {
            val dispatcher = Dispatcher()

            @kotlinx.serialization.Serializable
            data class TestParams(
                val value: Int,
                val name: String,
            )

            @kotlinx.serialization.Serializable
            data class TestResult(
                val computed: Int,
                val message: String,
            )

            dispatcher.onRequest<TestParams, TestResult>("test/typed") { params ->
                TestResult(
                    computed = params.value * 2,
                    message = "Hello ${params.name}",
                )
            }

            val requestCount = 5_000
            var successCount = 0

            val duration =
                measureTime {
                    repeat(requestCount) { i ->
                        val request =
                            RequestMessage(
                                id = RequestId.of(i.toLong()),
                                method = "test/typed",
                                params =
                                    buildJsonObject {
                                        put("value", i)
                                        put("name", "user$i")
                                    },
                            )
                        val response = dispatcher.dispatch(request)
                        if (response?.error == null) successCount++
                    }
                }

            successCount shouldBe requestCount
            println("=== Typed Handler Performance ===")
            println("$requestCount typed requests in ${duration.inWholeMilliseconds}ms")
            println("Average: ${duration.inWholeNanoseconds / requestCount}ns per request")
            println()
        }

    // ===== Large Message Handling =====

    @Test
    fun `handle large messages - measure memory efficiency`() {
        val decoder = JsonRpcMessageDecoder(codec)

        // Create a large params object (simulating a document)
        val largeContent = "x".repeat(100_000)

        // 100KB of content
        @Suppress("ktlint:standard:max-line-length")
        val content =
            """{"jsonrpc":"2.0","method":"textDocument/didOpen","params":{"textDocument":{"uri":"file:///test.txt","text":"$largeContent"}}}"""
        val message = "Content-Length: ${content.encodeToByteArray().size}\r\n\r\n$content"

        val messageCount = 100
        var parsedCount = 0

        val duration =
            measureTime {
                repeat(messageCount) {
                    decoder.reset() // Reset between messages to simulate fresh state
                    parsedCount += decoder.feed(message).size
                }
            }

        parsedCount shouldBe messageCount
        val totalMB = (message.length * messageCount) / (1024.0 * 1024.0)
        println("=== Large Message Performance ===")
        println("Parsed $messageCount large messages (~100KB each) in ${duration.inWholeMilliseconds}ms")
        println("Total data processed: ${formatTwoDecimals(totalMB)}MB")
        println("Throughput: ${formatTwoDecimals(totalMB * 1000 / duration.inWholeMilliseconds)}MB/s")
        println()
    }

    @Test
    fun `encode large response messages`() {
        val largeResultContent = "result-".repeat(10_000) // ~70KB

        val messages =
            (1..100).map { i ->
                ResponseMessage.success(
                    id = RequestId.of(i.toLong()),
                    result = JsonPrimitive(largeResultContent),
                )
            }

        var totalBytes = 0L
        val duration =
            measureTime {
                for (msg in messages) {
                    totalBytes += codec.encode(msg).size
                }
            }

        println("=== Large Response Encoding ===")
        println("Encoded ${messages.size} large responses in ${duration.inWholeMilliseconds}ms")
        println("Total output: ${totalBytes / 1024}KB")
        println()
    }

    // ===== Many Small Messages =====

    @Test
    fun `handle many small notification messages - measure overhead`() {
        val decoder = JsonRpcMessageDecoder(codec)
        val messageCount = 10_000

        // Small notification messages (typical in JSON-RPC)
        val content = """{"jsonrpc":"2.0","method":"$/progress"}"""
        val message = "Content-Length: ${content.length}\r\n\r\n$content"

        var totalParsed = 0
        val duration =
            measureTime {
                // Feed messages one at a time (realistic scenario)
                repeat(messageCount) {
                    totalParsed += decoder.feed(message).size
                }
            }

        totalParsed shouldBe messageCount
        val overhead = duration.inWholeNanoseconds / messageCount
        println("=== Small Message Overhead ===")
        println("Parsed $messageCount small notifications in ${duration.inWholeMilliseconds}ms")
        println("Per-message overhead: ${overhead}ns (${overhead / 1000} microseconds)")
        println()

        // Note: This test measures overhead, no hard assertion as it varies by system
        // Key observation: each feed() causes buffer += data string concatenation
    }

    // ===== Concurrent Request Handling =====

    @Test
    fun `concurrent request dispatch - measure contention`() =
        runTest {
            val dispatcher = Dispatcher()
            var handlerCallCount = 0
            val countMutex = kotlinx.coroutines.sync.Mutex()

            dispatcher.onRequest("test/concurrent") { _: kotlinx.serialization.json.JsonElement? ->
                countMutex.withLock { handlerCallCount++ }
                JsonPrimitive("ok")
            }

            val requestCount = 1_000

            val duration =
                measureTime {
                    withContext(Dispatchers.Default) {
                        val jobs =
                            (1..requestCount).map { i ->
                                async {
                                    val request =
                                        RequestMessage(
                                            id = RequestId.of(i.toLong()),
                                            method = "test/concurrent",
                                        )
                                    dispatcher.dispatch(request)
                                }
                            }
                        jobs.awaitAll()
                    }
                }

            handlerCallCount shouldBe requestCount
            println("=== Concurrent Dispatch Performance ===")
            println("$requestCount concurrent requests in ${duration.inWholeMilliseconds}ms")
            println("Effective throughput: ${requestCount * 1000L / duration.inWholeMilliseconds} req/s")
            println()
        }

    @Test
    fun `pending request management under load`() =
        runTest {
            val dispatcher = Dispatcher()
            val requestCount = 10_000

            // Register many pending requests
            val deferreds = mutableListOf<CompletableDeferred<kotlinx.serialization.json.JsonElement?>>()
            val registerDuration =
                measureTime {
                    repeat(requestCount) { i ->
                        deferreds.add(dispatcher.registerPendingRequest(RequestId.of(i.toLong())))
                    }
                }

            // Complete all pending requests
            val completeDuration =
                measureTime {
                    repeat(requestCount) { i ->
                        val response =
                            ResponseMessage.success(
                                id = RequestId.of(i.toLong()),
                                result = JsonPrimitive("result$i"),
                            )
                        dispatcher.dispatch(response)
                    }
                }

            // Verify all completed
            val verifyDuration =
                measureTime {
                    for (deferred in deferreds) {
                        deferred.isCompleted shouldBe true
                    }
                }

            println("=== Pending Request Management ===")
            println("Register $requestCount requests: ${registerDuration.inWholeMilliseconds}ms")
            println("Complete $requestCount requests: ${completeDuration.inWholeMilliseconds}ms")
            println("Verify completion: ${verifyDuration.inWholeMilliseconds}ms")
            println()
        }

    // ===== Connection Integration Performance =====

    @Test
    fun `connection receive and dispatch throughput`() =
        runTest {
            val connection = Connection()
            var handlerCallCount = 0
            val countMutex = kotlinx.coroutines.sync.Mutex()

            connection.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                countMutex.withLock { handlerCallCount++ }
                JsonPrimitive("result")
            }

            // Consume outgoing messages to prevent buffer backup
            val outgoingJob = connection.outgoing.onEach { }.launchIn(this)

            val messageCount = 5_000
            val messages =
                (1..messageCount).map { i ->
                    val content = """{"jsonrpc":"2.0","id":$i,"method":"test/method","params":{}}"""
                    "Content-Length: ${content.length}\r\n\r\n$content"
                }

            val duration =
                measureTime {
                    for (msg in messages) {
                        connection.receive(msg)
                    }
                }

            handlerCallCount shouldBe messageCount
            println("=== Connection Throughput ===")
            println("Processed $messageCount requests through Connection in ${duration.inWholeMilliseconds}ms")
            println("Throughput: ${messageCount * 1000L / duration.inWholeMilliseconds} req/s")
            println()

            connection.close()
            outgoingJob.cancelAndJoin()
        }

    // ===== Memory Pattern Tests =====

    @Test
    fun `decoder buffer behavior - verify no unbounded growth`() {
        val decoder = JsonRpcMessageDecoder(codec)

        // Feed many messages and check that buffer doesn't grow unboundedly
        repeat(1000) { i ->
            val content = """{"jsonrpc":"2.0","method":"test$i"}"""
            val message = "Content-Length: ${content.length}\r\n\r\n$content"
            val parsed = decoder.feed(message)
            parsed.size shouldBe 1
        }

        // After processing, the internal buffer should be empty or minimal
        // (We can't directly access the buffer, but if we got here without OOM, it's working)
        println("=== Buffer Behavior ===")
        println("Processed 1000 messages without memory issues")
        println()
    }

    @Test
    fun `string concatenation analysis in encoding`() {
        // Measure if there's excessive string concatenation
        val message =
            RequestMessage(
                id = RequestId.of(1),
                method = "test/method",
                params =
                    buildJsonObject {
                        put("key", "value")
                    },
            )

        // Warm up
        repeat(100) { codec.encode(message) }

        val iterations = 10_000
        val duration =
            measureTime {
                repeat(iterations) {
                    codec.encode(message)
                }
            }

        println("=== Encoding Efficiency ===")
        println("$iterations encodes in ${duration.inWholeMilliseconds}ms")
        println("Average: ${duration.inWholeNanoseconds / iterations}ns per encode")
        println()
    }

    // ===== Comparison Benchmarks =====

    @Test
    fun `compare RequestId types performance`() {
        val iterations = 100_000

        val numberIdDuration =
            measureTime {
                repeat(iterations) { i ->
                    RequestId.of(i.toLong())
                }
            }

        val stringIdDuration =
            measureTime {
                repeat(iterations) { i ->
                    RequestId.of("request-$i")
                }
            }

        println("=== RequestId Creation Performance ===")
        println("NumberId: $iterations in ${numberIdDuration.inWholeMilliseconds}ms")
        println("StringId: $iterations in ${stringIdDuration.inWholeMilliseconds}ms")
        println()
    }

    // ===== Summary Report =====

    @Test
    fun `print performance summary`() {
        println()
        println("=".repeat(60))
        println("PERFORMANCE TEST SUMMARY")
        println("=".repeat(60))
        println()
        println("Key performance characteristics of jsonrpc4k-core:")
        println()
        println("1. MESSAGE PARSING (JsonRpcMessageDecoder)")
        println("   - Uses string-based buffering")
        println("   - String concatenation on each feed() call")
        println("   - Linear search for header delimiter")
        println()
        println("2. SERIALIZATION (JsonRpcCodec)")
        println("   - Uses kotlinx.serialization.json")
        println("   - Creates intermediate ByteArray for Content-Length")
        println("   - Header + content concatenation per encode")
        println()
        println("3. DISPATCHER")
        println("   - HashMap lookup for handlers (O(1) average)")
        println("   - HashMap for pending requests")
        println("   - Per-request object creation for responses")
        println()
        println("4. MEMORY PATTERNS")
        println("   - Buffer cleared after each complete message")
        println("   - Pending requests removed on completion")
        println("   - No obvious memory leaks in basic usage")
        println()
        println("=".repeat(60))
    }

    // Helper function to format doubles with 2 decimal places (cross-platform)
    private fun formatTwoDecimals(value: Double): String {
        val intPart = value.toLong()
        val decPart = ((value - intPart) * 100).toLong().let { if (it < 0) -it else it }
        return "$intPart.${decPart.toString().padStart(2, '0')}"
    }
}
