package io.jsonrpc4k.core

import io.kotest.matchers.collections.shouldHaveSize
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.time.Duration.Companion.milliseconds

class ConnectionTest {
    // ===== Request Handler Registration Tests =====

    @Test
    fun `onRequest registers handler that responds to requests`() =
        runTest {
            val connection = Connection()
            connection.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                JsonPrimitive("result")
            }

            // Simulate incoming request
            val requestContent = """{"jsonrpc":"2.0","id":1,"method":"test/method"}"""
            val input = "Content-Length: ${requestContent.length}\r\n\r\n$requestContent"

            // Collect the outgoing response
            val responseJob = async { connection.outgoing.first() }
            connection.receive(input)
            val responseBytes = responseJob.await()
            val responseStr = responseBytes.decodeToString()

            responseStr shouldContain "\"result\":\"result\""
            responseStr shouldContain "\"id\":1"

            connection.close()
        }

    @Test
    fun `onNotification registers handler that handles notifications`() =
        runTest {
            val connection = Connection()
            var notificationReceived = false
            connection.onNotification("test/notify") { _: kotlinx.serialization.json.JsonElement? ->
                notificationReceived = true
            }

            // Simulate incoming notification
            val notificationContent = """{"jsonrpc":"2.0","method":"test/notify"}"""
            val input = "Content-Length: ${notificationContent.length}\r\n\r\n$notificationContent"
            connection.receive(input)

            notificationReceived shouldBe true

            connection.close()
        }

    // ===== Typed Request Handler Tests =====

    @Serializable
    data class EchoParams(
        val message: String,
    )

    @Serializable
    data class EchoResult(
        val echo: String,
    )

    @Test
    fun `typed onRequest handler works with serializable types`() =
        runTest {
            val connection = Connection()
            connection.onRequest<EchoParams, EchoResult>("echo") { params ->
                EchoResult(echo = "Echo: ${params.message}")
            }

            val requestContent = """{"jsonrpc":"2.0","id":1,"method":"echo","params":{"message":"Hello"}}"""
            val input = "Content-Length: ${requestContent.length}\r\n\r\n$requestContent"

            val responseJob = async { connection.outgoing.first() }
            connection.receive(input)
            val responseBytes = responseJob.await()
            val responseStr = responseBytes.decodeToString()

            responseStr shouldContain "\"echo\":\"Echo: Hello\""

            connection.close()
        }

    // ===== Typed Notification Handler Tests =====

    @Serializable
    data class LogParams(
        val level: String,
        val message: String,
    )

    @Test
    fun `typed onNotification handler works with serializable types`() =
        runTest {
            val connection = Connection()
            var receivedLog: LogParams? = null
            connection.onNotification<LogParams>("log") { params ->
                receivedLog = params
            }

            val notificationContent = """{"jsonrpc":"2.0","method":"log","params":{"level":"info","message":"Test"}}"""
            val input = "Content-Length: ${notificationContent.length}\r\n\r\n$notificationContent"
            connection.receive(input)

            receivedLog shouldNotBe null
            receivedLog!!.level shouldBe "info"
            receivedLog!!.message shouldBe "Test"

            connection.close()
        }

    // ===== Sending Requests Tests =====

    @Test
    fun `request sends message and receives response`() =
        runTest {
            val connection = Connection()

            // Collect outgoing request
            val outgoingJob = async { connection.outgoing.first() }

            // Send request
            val requestJob =
                async {
                    connection.request<JsonPrimitive>("test/method", JsonPrimitive("param"))
                }

            // Get the outgoing request
            val requestBytes = outgoingJob.await()
            val requestStr = requestBytes.decodeToString()

            requestStr shouldContain "\"method\":\"test/method\""
            requestStr shouldContain "\"id\":1" // First request ID

            // Simulate response
            val responseContent = """{"jsonrpc":"2.0","id":1,"result":"success"}"""
            val responseInput = "Content-Length: ${responseContent.length}\r\n\r\n$responseContent"
            connection.receive(responseInput)

            // Get result
            val result = requestJob.await()
            result shouldBe JsonPrimitive("success")

            connection.close()
        }

    @Test
    fun `request generates unique sequential IDs`() =
        runTest {
            val connection = Connection()

            // Collect multiple outgoing requests
            val requests = mutableListOf<String>()

            val collector =
                launch {
                    connection.outgoing.take(3).collect { bytes ->
                        requests.add(bytes.decodeToString())
                    }
                }

            // Send multiple requests without waiting for responses
            // Use very short timeout and catch the exception since we don't care about responses
            val req1 =
                launch {
                    try {
                        connection.request<JsonPrimitive>("method1", null, timeout = 50.milliseconds)
                    } catch (_: JsonRpcException) {
                        // Expected timeout
                    }
                }
            val req2 =
                launch {
                    try {
                        connection.request<JsonPrimitive>("method2", null, timeout = 50.milliseconds)
                    } catch (_: JsonRpcException) {
                        // Expected timeout
                    }
                }
            val req3 =
                launch {
                    try {
                        connection.request<JsonPrimitive>("method3", null, timeout = 50.milliseconds)
                    } catch (_: JsonRpcException) {
                        // Expected timeout
                    }
                }

            // Wait for requests to be sent and collector to finish
            collector.join()

            requests shouldHaveSize 3

            // Extract IDs from requests
            val ids =
                requests
                    .map { req ->
                        val match = Regex("\"id\":(\\d+)").find(req)
                        match?.groupValues?.get(1)?.toInt()
                    }.filterNotNull()
                    .sorted()

            // IDs should be sequential
            ids shouldBe listOf(1, 2, 3)

            // Cancel requests and close
            req1.cancel()
            req2.cancel()
            req3.cancel()
            connection.close()
        }

    @Test
    fun `request with error response throws JsonRpcException`() =
        runTest {
            val connection = Connection()

            val requestJob =
                async {
                    assertFailsWith<JsonRpcException> {
                        // Start the request
                        launch {
                            kotlinx.coroutines.delay(50)
                            // Simulate error response
                            val responseContent =
                                """{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"Method not found"}}"""
                            val responseInput = "Content-Length: ${responseContent.length}\r\n\r\n$responseContent"
                            connection.receive(responseInput)
                        }
                        connection.request<JsonPrimitive>("unknown/method", null)
                    }
                }

            // Collect and discard outgoing request
            launch { connection.outgoing.first() }

            val exception = requestJob.await()
            exception.code shouldBe ResponseError.METHOD_NOT_FOUND

            connection.close()
        }

    // ===== Typed Request Tests =====

    @Serializable
    data class AddParams(
        val a: Int,
        val b: Int,
    )

    @Serializable
    data class AddResult(
        val sum: Int,
    )

    @Test
    fun `typed request sends serialized params`() =
        runTest {
            val connection = Connection()

            val outgoingJob = async { connection.outgoing.first() }

            launch {
                kotlinx.coroutines.delay(50)
                // Simulate response
                val responseContent = """{"jsonrpc":"2.0","id":1,"result":{"sum":42}}"""
                val responseInput = "Content-Length: ${responseContent.length}\r\n\r\n$responseContent"
                connection.receive(responseInput)
            }

            val result = async { connection.request<AddParams, AddResult>("add", AddParams(a = 20, b = 22)) }

            val requestStr = outgoingJob.await().decodeToString()
            requestStr shouldContain "\"a\":20"
            requestStr shouldContain "\"b\":22"

            val addResult = result.await()
            addResult shouldNotBe null
            addResult!!.sum shouldBe 42

            connection.close()
        }

    // ===== Sending Notifications Tests =====

    @Test
    fun `notify sends notification message`() =
        runTest {
            val connection = Connection()

            val outgoingJob = async { connection.outgoing.first() }

            connection.notify("test/notification", JsonPrimitive("data"))

            val notificationStr = outgoingJob.await().decodeToString()
            notificationStr shouldContain "\"method\":\"test/notification\""
            notificationStr shouldContain "\"params\":\"data\""
            // Notifications should NOT have an id
            notificationStr.contains("\"id\":").shouldBe(false)

            connection.close()
        }

    @Test
    fun `typed notify sends serialized notification`() =
        runTest {
            val connection = Connection()

            val outgoingJob = async { connection.outgoing.first() }

            connection.notify("log", LogParams(level = "error", message = "Something failed"))

            val notificationStr = outgoingJob.await().decodeToString()
            notificationStr shouldContain "\"level\":\"error\""
            notificationStr shouldContain "\"message\":\"Something failed\""

            connection.close()
        }

    @Test
    fun `notify with null params`() =
        runTest {
            val connection = Connection()

            val outgoingJob = async { connection.outgoing.first() }

            connection.notify("test/empty", null)

            val notificationStr = outgoingJob.await().decodeToString()
            notificationStr shouldContain "\"method\":\"test/empty\""

            connection.close()
        }

    // ===== Receive Data Tests =====

    @Test
    fun `receive handles string data`() =
        runTest {
            val connection = Connection()
            var handlerCalled = false
            connection.onNotification("test") { _: kotlinx.serialization.json.JsonElement? ->
                handlerCalled = true
            }

            val content = """{"jsonrpc":"2.0","method":"test"}"""
            val input = "Content-Length: ${content.length}\r\n\r\n$content"
            connection.receive(input)

            handlerCalled shouldBe true

            connection.close()
        }

    @Test
    fun `receive handles byte array data`() =
        runTest {
            val connection = Connection()
            var handlerCalled = false
            connection.onNotification("test") { _: kotlinx.serialization.json.JsonElement? ->
                handlerCalled = true
            }

            val content = """{"jsonrpc":"2.0","method":"test"}"""
            val input = "Content-Length: ${content.length}\r\n\r\n$content"
            connection.receive(input.encodeToByteArray())

            handlerCalled shouldBe true

            connection.close()
        }

    @Test
    fun `receive handles multiple messages in single call`() =
        runTest {
            val connection = Connection()
            val receivedMethods = mutableListOf<String>()
            connection.onNotification("method1") { _: kotlinx.serialization.json.JsonElement? ->
                receivedMethods.add("method1")
            }
            connection.onNotification("method2") { _: kotlinx.serialization.json.JsonElement? ->
                receivedMethods.add("method2")
            }

            val content1 = """{"jsonrpc":"2.0","method":"method1"}"""
            val content2 = """{"jsonrpc":"2.0","method":"method2"}"""
            val input =
                "Content-Length: ${content1.length}\r\n\r\n$content1" +
                    "Content-Length: ${content2.length}\r\n\r\n$content2"

            connection.receive(input)

            receivedMethods shouldHaveSize 2
            receivedMethods shouldBe listOf("method1", "method2")

            connection.close()
        }

    @Test
    fun `receive handles partial messages across calls`() =
        runTest {
            val connection = Connection()
            var handlerCalled = false
            connection.onNotification("test") { _: kotlinx.serialization.json.JsonElement? ->
                handlerCalled = true
            }

            val content = """{"jsonrpc":"2.0","method":"test"}"""
            val fullInput = "Content-Length: ${content.length}\r\n\r\n$content"

            // Split the input
            connection.receive(fullInput.substring(0, 20))
            handlerCalled shouldBe false

            connection.receive(fullInput.substring(20))
            handlerCalled shouldBe true

            connection.close()
        }

    // ===== Request Response Flow Tests =====

    @Test
    fun `incoming request triggers outgoing response`() =
        runTest {
            val connection = Connection()
            connection.onRequest("greet") { params: kotlinx.serialization.json.JsonElement? ->
                val name =
                    params
                        ?.jsonObject
                        ?.get("name")
                        ?.jsonPrimitive
                        ?.content ?: "World"
                buildJsonObject { put("greeting", JsonPrimitive("Hello, $name!")) }
            }

            val requestContent = """{"jsonrpc":"2.0","id":42,"method":"greet","params":{"name":"Alice"}}"""
            val input = "Content-Length: ${requestContent.length}\r\n\r\n$requestContent"

            val responseJob = async { connection.outgoing.first() }
            connection.receive(input)
            val responseStr = responseJob.await().decodeToString()

            responseStr shouldContain "\"id\":42"
            responseStr shouldContain "\"greeting\":\"Hello, Alice!\""

            connection.close()
        }

    @Test
    fun `incoming request with error triggers error response`() =
        runTest {
            val connection = Connection()
            connection.onRequest("fail") { _: kotlinx.serialization.json.JsonElement? ->
                throw JsonRpcException(ResponseError.INVALID_PARAMS, "Bad params")
            }

            val requestContent = """{"jsonrpc":"2.0","id":1,"method":"fail"}"""
            val input = "Content-Length: ${requestContent.length}\r\n\r\n$requestContent"

            val responseJob = async { connection.outgoing.first() }
            connection.receive(input)
            val responseStr = responseJob.await().decodeToString()

            responseStr shouldContain "\"id\":1"
            responseStr shouldContain "\"error\":"
            // Error codes are numbers, not strings
            responseStr shouldContain "\"code\":-32602"

            connection.close()
        }

    @Test
    fun `unregistered method returns method not found`() =
        runTest {
            val connection = Connection()

            val requestContent = """{"jsonrpc":"2.0","id":1,"method":"unknown/method"}"""
            val input = "Content-Length: ${requestContent.length}\r\n\r\n$requestContent"

            val responseJob = async { connection.outgoing.first() }
            connection.receive(input)
            val responseStr = responseJob.await().decodeToString()

            responseStr shouldContain "\"error\":"
            // Error codes are numbers, not strings
            responseStr shouldContain "\"code\":-32601"

            connection.close()
        }

    // ===== Connection Close Tests =====

    @Test
    fun `close terminates the connection`() =
        runTest {
            val connection = Connection()
            connection.close()
            // After close, the outgoing channel should be closed
            // We can verify this by checking that collecting throws or completes immediately
        }

    // ===== Outgoing Flow Tests =====

    @Test
    fun `outgoing flow emits encoded messages`() =
        runTest {
            val connection = Connection()

            val outgoingJob = async { connection.outgoing.first() }

            connection.notify("test", null)

            val bytes = outgoingJob.await()
            val str = bytes.decodeToString()

            str shouldContain "Content-Length:"
            str shouldContain "\r\n\r\n"
            str shouldContain "\"method\":\"test\""

            connection.close()
        }

    @Test
    fun `outgoing flow emits multiple messages in order`() =
        runTest {
            val connection = Connection()

            val messagesJob =
                async {
                    connection.outgoing.take(3).toList()
                }

            connection.notify("first", null)
            connection.notify("second", null)
            connection.notify("third", null)

            val messages = messagesJob.await()
            messages shouldHaveSize 3

            messages[0].decodeToString() shouldContain "\"method\":\"first\""
            messages[1].decodeToString() shouldContain "\"method\":\"second\""
            messages[2].decodeToString() shouldContain "\"method\":\"third\""

            connection.close()
        }

    // ===== Integration Tests =====

    @Test
    fun `full request response cycle`() =
        runTest {
            val serverConnection = Connection()
            val clientConnection = Connection()

            // Set up server handler
            serverConnection.onRequest<AddParams, AddResult>("add") { params ->
                AddResult(sum = params.a + params.b)
            }

            // Simulate network: forward client outgoing to server incoming
            launch {
                clientConnection.outgoing.collect { bytes ->
                    serverConnection.receive(bytes)
                }
            }

            // Simulate network: forward server outgoing to client incoming
            launch {
                serverConnection.outgoing.collect { bytes ->
                    clientConnection.receive(bytes)
                }
            }

            // Client makes request
            val result = clientConnection.request<AddParams, AddResult>("add", AddParams(a = 10, b = 32))

            result shouldNotBe null
            result!!.sum shouldBe 42

            serverConnection.close()
            clientConnection.close()
        }

    @Test
    fun `bidirectional notification flow`() =
        runTest {
            val serverConnection = Connection()
            val clientConnection = Connection()

            var serverReceivedPing = false
            var clientReceivedPong = false

            serverConnection.onNotification("ping") { _: kotlinx.serialization.json.JsonElement? ->
                serverReceivedPing = true
                // Server sends pong back
                serverConnection.notify("pong", null)
            }

            clientConnection.onNotification("pong") { _: kotlinx.serialization.json.JsonElement? ->
                clientReceivedPong = true
            }

            // Simulate bidirectional network
            launch {
                clientConnection.outgoing.collect { bytes ->
                    serverConnection.receive(bytes)
                }
            }

            launch {
                serverConnection.outgoing.collect { bytes ->
                    clientConnection.receive(bytes)
                }
            }

            // Client sends ping
            clientConnection.notify("ping", null)

            // Wait for propagation
            kotlinx.coroutines.delay(100)

            serverReceivedPing shouldBe true
            clientReceivedPong shouldBe true

            serverConnection.close()
            clientConnection.close()
        }
}
