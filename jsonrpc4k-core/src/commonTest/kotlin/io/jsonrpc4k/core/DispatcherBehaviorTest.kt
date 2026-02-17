package io.jsonrpc4k.core

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.put
import kotlin.test.Test

/**
 * Behavioral tests for Dispatcher.
 * Tests handler registration, dispatch behavior, and error handling.
 */
class DispatcherBehaviorTest {
    // ==================== Handler Registration Tests ====================

    @Test
    fun `onRequest registers and dispatches request handler`() =
        runTest {
            val dispatcher = Dispatcher()
            var called = false

            dispatcher.onRequest("test/method") { _: JsonElement? ->
                called = true
                JsonPrimitive("response")
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/method",
                )
            val response = dispatcher.dispatch(request)

            called shouldBe true
            response shouldNotBe null
        }

    @Test
    fun `onNotification registers and dispatches notification handler`() =
        runTest {
            val dispatcher = Dispatcher()
            var called = false

            dispatcher.onNotification("test/notification") { _: JsonElement? ->
                called = true
            }

            val notification = NotificationMessage(method = "test/notification")
            dispatcher.dispatch(notification)

            called shouldBe true
        }

    @Test
    fun `unhandled request returns method not found error`() =
        runTest {
            val dispatcher = Dispatcher()

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "unknown/method",
                )
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response?.error?.code shouldBe ResponseError.METHOD_NOT_FOUND
        }

    @Test
    fun `unhandled notification is silently ignored`() =
        runTest {
            val dispatcher = Dispatcher()

            // Should not throw
            val notification = NotificationMessage(method = "unknown/notification")
            dispatcher.dispatch(notification)
        }

    // ==================== Typed Handler Tests ====================

    @Serializable
    data class TestParams(
        val value: Int,
        val name: String,
    )

    @Serializable
    data class TestResult(
        val computed: Int,
        val message: String,
    )

    @Test
    fun `typed onRequest deserializes params and serializes result`() =
        runTest {
            val dispatcher = Dispatcher()

            dispatcher.onRequest<TestParams, TestResult>("test/typed") { params ->
                TestResult(
                    computed = params.value * 2,
                    message = "Hello ${params.name}",
                )
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/typed",
                    params =
                        buildJsonObject {
                            put("value", 10)
                            put("name", "World")
                        },
                )
            val response = dispatcher.dispatch(request)

            response?.error shouldBe null
            response?.result?.toString() shouldBe """{"computed":20,"message":"Hello World"}"""
        }

    @Test
    fun `typed onNotification deserializes params`() =
        runTest {
            val dispatcher = Dispatcher()
            var receivedParams: TestParams? = null

            dispatcher.onNotification<TestParams>("test/typedNotification") { params ->
                receivedParams = params
            }

            val notification =
                NotificationMessage(
                    method = "test/typedNotification",
                    params =
                        buildJsonObject {
                            put("value", 42)
                            put("name", "Test")
                        },
                )
            dispatcher.dispatch(notification)

            receivedParams shouldNotBe null
            receivedParams?.value shouldBe 42
            receivedParams?.name shouldBe "Test"
        }

    @Test
    fun `typed handler with invalid params returns error`() =
        runTest {
            val dispatcher = Dispatcher()

            dispatcher.onRequest<TestParams, TestResult>("test/typed") { params ->
                TestResult(params.value, params.name)
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/typed",
                    params =
                        buildJsonObject {
                            // Missing required fields
                            put("wrongField", "value")
                        },
                )
            val response = dispatcher.dispatch(request)

            response?.error shouldNotBe null
        }

    // ==================== Handler Exception Tests ====================

    @Test
    fun `request handler exception returns internal error`() =
        runTest {
            val dispatcher = Dispatcher()

            dispatcher.onRequest("test/throws") { _: JsonElement? ->
                throw IllegalStateException("Handler error")
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/throws",
                )
            val response = dispatcher.dispatch(request)

            response?.error shouldNotBe null
            response?.error?.code shouldBe ResponseError.INTERNAL_ERROR
        }

    @Test
    fun `notification handler exception does not propagate without error handler`() =
        runTest {
            val dispatcher = Dispatcher()

            dispatcher.onNotification("test/throws") { _: JsonElement? ->
                throw IllegalStateException("Handler error")
            }

            // Should not throw
            val notification = NotificationMessage(method = "test/throws")
            dispatcher.dispatch(notification)
        }

    @Test
    fun `notification handler exception is reported to error handler`() =
        runTest {
            var reportedMethod: String? = null
            var reportedException: Exception? = null
            val dispatcher =
                Dispatcher(
                    notificationErrorHandler =
                        NotificationErrorHandler { method, exception ->
                            reportedMethod = method
                            reportedException = exception
                        },
                )

            dispatcher.onNotification("test/throws") { _: JsonElement? ->
                throw IllegalStateException("Handler error")
            }

            val notification = NotificationMessage(method = "test/throws")
            dispatcher.dispatch(notification)

            reportedMethod shouldBe "test/throws"
            reportedException shouldNotBe null
            reportedException!!.message shouldBe "Handler error"
        }

    @Test
    fun `JsonRpcException is properly serialized`() =
        runTest {
            val dispatcher = Dispatcher()

            dispatcher.onRequest("test/jsonrpcError") { _: JsonElement? ->
                throw JsonRpcException(
                    code = ResponseError.INVALID_PARAMS,
                    message = "Custom error",
                )
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/jsonrpcError",
                )
            val response = dispatcher.dispatch(request)

            response?.error?.code shouldBe ResponseError.INVALID_PARAMS
            response?.error?.message shouldBe "Custom error"
        }

    // ==================== Pending Request Tests ====================

    @Test
    fun `registerPendingRequest creates deferred`() =
        runTest {
            val dispatcher = Dispatcher()
            val deferred = dispatcher.registerPendingRequest(RequestId.of(1))

            deferred.isCompleted shouldBe false
        }

    @Test
    fun `response completes pending request`() =
        runTest {
            val dispatcher = Dispatcher()
            val deferred = dispatcher.registerPendingRequest(RequestId.of(1))

            val response =
                ResponseMessage.success(
                    id = RequestId.of(1),
                    result = JsonPrimitive("result"),
                )
            dispatcher.dispatch(response)

            deferred.isCompleted shouldBe true
            deferred.await() shouldBe JsonPrimitive("result")
        }

    @Test
    fun `error response completes pending request with exception`() =
        runTest {
            val dispatcher = Dispatcher()
            val deferred = dispatcher.registerPendingRequest(RequestId.of(1))

            val response =
                ResponseMessage.error(
                    id = RequestId.of(1),
                    error = ResponseError.invalidParams("Error message"),
                )
            dispatcher.dispatch(response)

            deferred.isCompleted shouldBe true
            try {
                deferred.await()
            } catch (e: JsonRpcException) {
                e.code shouldBe ResponseError.INVALID_PARAMS
                e.message shouldBe "Error message"
            }
        }

    @Test
    fun `response for unknown request is ignored`() =
        runTest {
            val dispatcher = Dispatcher()

            // No pending request registered
            val response =
                ResponseMessage.success(
                    id = RequestId.of(999),
                    result = JsonPrimitive("result"),
                )

            // Should not throw
            dispatcher.dispatch(response)
        }

    // ==================== Multiple Handlers Tests ====================

    @Test
    fun `multiple handlers can be registered`() =
        runTest {
            val dispatcher = Dispatcher()
            val called = mutableListOf<String>()

            dispatcher.onRequest("method1") { _: JsonElement? ->
                called.add("method1")
                JsonPrimitive("result1")
            }
            dispatcher.onRequest("method2") { _: JsonElement? ->
                called.add("method2")
                JsonPrimitive("result2")
            }
            dispatcher.onNotification("notify1") { _: JsonElement? ->
                called.add("notify1")
            }

            dispatcher.dispatch(RequestMessage(id = RequestId.of(1), method = "method1"))
            dispatcher.dispatch(RequestMessage(id = RequestId.of(2), method = "method2"))
            dispatcher.dispatch(NotificationMessage(method = "notify1"))

            called shouldBe listOf("method1", "method2", "notify1")
        }

    @Test
    fun `replacing handler overwrites previous`() =
        runTest {
            val dispatcher = Dispatcher()
            var firstCalled = false
            var secondCalled = false

            dispatcher.onRequest("test/method") { _: JsonElement? ->
                firstCalled = true
                JsonPrimitive("first")
            }
            dispatcher.onRequest("test/method") { _: JsonElement? ->
                secondCalled = true
                JsonPrimitive("second")
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/method",
                )
            dispatcher.dispatch(request)

            firstCalled shouldBe false
            secondCalled shouldBe true
        }

    // ==================== Params Access Tests ====================

    @Test
    fun `handler receives params correctly`() =
        runTest {
            val dispatcher = Dispatcher()
            var receivedParams: JsonElement? = null

            dispatcher.onRequest("test/params") { params: JsonElement? ->
                receivedParams = params
                JsonPrimitive("ok")
            }

            val params =
                buildJsonObject {
                    put("key", "value")
                    put("number", 42)
                }
            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/params",
                    params = params,
                )
            dispatcher.dispatch(request)

            receivedParams shouldBe params
        }

    @Test
    fun `handler receives null params when not provided`() =
        runTest {
            val dispatcher = Dispatcher()
            var receivedParams: JsonElement? = JsonPrimitive("initial")
            var wasCalled = false

            dispatcher.onRequest("test/noParams") { params: JsonElement? ->
                wasCalled = true
                receivedParams = params
                JsonPrimitive("ok")
            }

            val request =
                RequestMessage(
                    id = RequestId.of(1),
                    method = "test/noParams",
                    // No params
                )
            dispatcher.dispatch(request)

            wasCalled shouldBe true
            receivedParams shouldBe null
        }
}
