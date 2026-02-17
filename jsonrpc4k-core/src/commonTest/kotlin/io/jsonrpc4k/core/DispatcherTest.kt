package io.jsonrpc4k.core

import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlin.test.Test
import kotlin.test.assertFailsWith

class DispatcherTest {
    private val dispatcher = Dispatcher()

    // ===== Request Handler Registration Tests =====

    @Test
    fun `onRequest registers handler for method`() =
        runTest {
            var handlerCalled = false
            dispatcher.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                handlerCalled = true
                JsonPrimitive("result")
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/method")
            dispatcher.dispatch(request)

            handlerCalled shouldBe true
        }

    @Test
    fun `onRequest handler receives params`() =
        runTest {
            var receivedParams: kotlinx.serialization.json.JsonElement? = null
            dispatcher.onRequest("test/method") { params: kotlinx.serialization.json.JsonElement? ->
                receivedParams = params
                null
            }

            val params = buildJsonObject { put("key", JsonPrimitive("value")) }
            val request = RequestMessage(id = RequestId.of(1), method = "test/method", params = params)
            dispatcher.dispatch(request)

            receivedParams shouldBe params
        }

    @Test
    fun `onRequest returns success response`() =
        runTest {
            dispatcher.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                JsonPrimitive("success")
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/method")
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.id shouldBe RequestId.of(1)
            response.result shouldBe JsonPrimitive("success")
            response.error shouldBe null
        }

    @Test
    fun `onRequest returns null result for void methods`() =
        runTest {
            dispatcher.onRequest("test/void") { _: kotlinx.serialization.json.JsonElement? ->
                null
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/void")
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.result shouldBe null
            response.error shouldBe null
        }

    @Test
    fun `unregistered method returns method not found error`() =
        runTest {
            val request = RequestMessage(id = RequestId.of(1), method = "unknown/method")
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.error shouldNotBe null
            response.error!!.code shouldBe ResponseError.METHOD_NOT_FOUND
            response.error!!.message shouldBe "Method not found: unknown/method"
        }

    @Test
    fun `handler exception returns internal error response with generic message by default`() =
        runTest {
            // By default, includeExceptionDetails is false for security (SEC-001)
            dispatcher.onRequest("test/error") { _: kotlinx.serialization.json.JsonElement? ->
                throw IllegalStateException("Something went wrong")
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/error")
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.error shouldNotBe null
            response.error!!.code shouldBe ResponseError.INTERNAL_ERROR
            response.error!!.message shouldBe "Internal error"
        }

    @Test
    fun `handler JsonRpcException returns custom error response`() =
        runTest {
            dispatcher.onRequest("test/jsonrpc-error") { _: kotlinx.serialization.json.JsonElement? ->
                throw JsonRpcException(ResponseError.INVALID_PARAMS, "Invalid parameters provided")
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/jsonrpc-error")
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.error shouldNotBe null
            response.error!!.code shouldBe ResponseError.INVALID_PARAMS
            response.error!!.message shouldBe "Invalid parameters provided"
        }

    @Test
    fun `handler JsonRpcException with data returns error with data`() =
        runTest {
            val errorData = buildJsonObject { put("detail", JsonPrimitive("extra info")) }
            dispatcher.onRequest("test/jsonrpc-error-data") { _: kotlinx.serialization.json.JsonElement? ->
                throw JsonRpcException(ResponseError.INVALID_PARAMS, "Invalid parameters", errorData)
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/jsonrpc-error-data")
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.error shouldNotBe null
            response.error!!.data shouldBe errorData
        }

    @Test
    fun `handler replaces previous handler for same method`() =
        runTest {
            dispatcher.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                JsonPrimitive("first")
            }
            dispatcher.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                JsonPrimitive("second")
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/method")
            val response = dispatcher.dispatch(request)

            response!!.result shouldBe JsonPrimitive("second")
        }

    // ===== Typed Request Handler Tests =====

    @Serializable
    data class TestParams(
        val name: String,
        val value: Int,
    )

    @Serializable
    data class TestResult(
        val message: String,
        val success: Boolean,
    )

    @Test
    fun `typed onRequest deserializes params and serializes result`() =
        runTest {
            dispatcher.onRequest<TestParams, TestResult>("test/typed") { params ->
                TestResult(message = "Hello ${params.name}", success = params.value > 0)
            }

            val params =
                buildJsonObject {
                    put("name", JsonPrimitive("World"))
                    put("value", JsonPrimitive(42))
                }
            val request = RequestMessage(id = RequestId.of(1), method = "test/typed", params = params)
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.result shouldNotBe null
            val result = response.result!!.jsonObject
            result["message"]?.jsonPrimitive?.content shouldBe "Hello World"
            result["success"]?.jsonPrimitive?.content shouldBe "true"
        }

    @Test
    fun `typed onRequest handles null params when nullable`() =
        runTest {
            dispatcher.onRequest<TestParams?, TestResult>("test/nullable") { params ->
                TestResult(
                    message = params?.name ?: "default",
                    success = true,
                )
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/nullable", params = null)
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.result shouldNotBe null
            val result = response.result!!.jsonObject
            result["message"]?.jsonPrimitive?.content shouldBe "default"
        }

    @Test
    fun `typed onRequest handles null result`() =
        runTest {
            dispatcher.onRequest<TestParams, TestResult?>("test/null-result") { _ ->
                null
            }

            val params =
                buildJsonObject {
                    put("name", JsonPrimitive("test"))
                    put("value", JsonPrimitive(1))
                }
            val request = RequestMessage(id = RequestId.of(1), method = "test/null-result", params = params)
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.result shouldBe null
            response.error shouldBe null
        }

    // ===== Notification Handler Tests =====

    @Test
    fun `onNotification registers handler for method`() =
        runTest {
            var handlerCalled = false
            dispatcher.onNotification("test/notify") { _: kotlinx.serialization.json.JsonElement? ->
                handlerCalled = true
            }

            val notification = NotificationMessage(method = "test/notify")
            dispatcher.dispatch(notification)

            handlerCalled shouldBe true
        }

    @Test
    fun `onNotification handler receives params`() =
        runTest {
            var receivedParams: kotlinx.serialization.json.JsonElement? = null
            dispatcher.onNotification("test/notify") { params: kotlinx.serialization.json.JsonElement? ->
                receivedParams = params
            }

            val params = buildJsonObject { put("event", JsonPrimitive("something")) }
            val notification = NotificationMessage(method = "test/notify", params = params)
            dispatcher.dispatch(notification)

            receivedParams shouldBe params
        }

    @Test
    fun `notification dispatch returns null`() =
        runTest {
            dispatcher.onNotification("test/notify") { _: kotlinx.serialization.json.JsonElement? -> }

            val notification = NotificationMessage(method = "test/notify")
            val response = dispatcher.dispatch(notification)

            response shouldBe null
        }

    @Test
    fun `unregistered notification is silently ignored`() =
        runTest {
            val notification = NotificationMessage(method = "unknown/notification")
            val response = dispatcher.dispatch(notification)

            response shouldBe null
        }

    @Test
    fun `notification handler exception is silently ignored without error handler`() =
        runTest {
            dispatcher.onNotification("test/error") { _: kotlinx.serialization.json.JsonElement? ->
                throw IllegalStateException("This should be caught")
            }

            val notification = NotificationMessage(method = "test/error")
            // Should not throw
            val response = dispatcher.dispatch(notification)

            response shouldBe null
        }

    // ===== Typed Notification Handler Tests =====

    @Serializable
    data class NotifyParams(
        val message: String,
    )

    @Test
    fun `typed onNotification deserializes params`() =
        runTest {
            var receivedMessage: String? = null
            dispatcher.onNotification<NotifyParams>("test/typed-notify") { params ->
                receivedMessage = params.message
            }

            val params = buildJsonObject { put("message", JsonPrimitive("Hello")) }
            val notification = NotificationMessage(method = "test/typed-notify", params = params)
            dispatcher.dispatch(notification)

            receivedMessage shouldBe "Hello"
        }

    @Test
    fun `typed onNotification handles null params when nullable`() =
        runTest {
            var handlerCalled = false
            dispatcher.onNotification<NotifyParams?>("test/nullable-notify") { params ->
                handlerCalled = true
                params shouldBe null
            }

            val notification = NotificationMessage(method = "test/nullable-notify", params = null)
            dispatcher.dispatch(notification)

            handlerCalled shouldBe true
        }

    @Test
    fun `typed onRequest returns error for null params when non-nullable`() =
        runTest {
            dispatcher.onRequest<TestParams, TestResult>("test/non-nullable") { params ->
                TestResult(message = params.name, success = true)
            }

            val request = RequestMessage(id = RequestId.of(1), method = "test/non-nullable", params = null)
            val response = dispatcher.dispatch(request)

            response shouldNotBe null
            response!!.error shouldNotBe null
            response.error!!.code shouldBe ResponseError.INVALID_PARAMS
            response.error!!.message shouldBe "Missing required params for method"
        }

    @Test
    fun `notification errors are silently ignored without error handler`() =
        runTest {
            // No error handler set - errors are silently dropped
            dispatcher.onNotification("test/error-notify") { _: kotlinx.serialization.json.JsonElement? ->
                throw IllegalStateException("This should be caught and ignored")
            }

            val notification = NotificationMessage(method = "test/error-notify")
            // Should not throw
            val response = dispatcher.dispatch(notification)

            response shouldBe null
        }

    @Test
    fun `notification errors are reported to error handler`() =
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

            dispatcher.onNotification("test/error-notify") { _: kotlinx.serialization.json.JsonElement? ->
                throw IllegalStateException("Handler failed")
            }

            val notification = NotificationMessage(method = "test/error-notify")
            dispatcher.dispatch(notification)

            reportedMethod shouldBe "test/error-notify"
            reportedException shouldNotBe null
            reportedException!!.message shouldBe "Handler failed"
        }

    // ===== Response Handling Tests =====

    @Test
    fun `response dispatch returns null`() =
        runTest {
            val response = ResponseMessage.success(id = RequestId.of(1), result = JsonPrimitive("ok"))
            val result = dispatcher.dispatch(response)

            result shouldBe null
        }

    @Test
    fun `response with pending request completes deferred`() =
        runTest {
            val id = RequestId.of(1)
            val deferred = dispatcher.registerPendingRequest(id)

            val response = ResponseMessage.success(id = id, result = JsonPrimitive("result"))
            dispatcher.dispatch(response)

            deferred.isCompleted shouldBe true
            deferred.await() shouldBe JsonPrimitive("result")
        }

    @Test
    fun `response with error completes deferred exceptionally`() =
        runTest {
            val id = RequestId.of(1)
            val deferred = dispatcher.registerPendingRequest(id)

            val response =
                ResponseMessage.error(
                    id = id,
                    error = ResponseError.methodNotFound("test/method"),
                )
            dispatcher.dispatch(response)

            deferred.isCompleted shouldBe true
            val exception = assertFailsWith<JsonRpcException> { deferred.await() }
            exception.code shouldBe ResponseError.METHOD_NOT_FOUND
        }

    @Test
    fun `response with null id is ignored`() =
        runTest {
            val response = ResponseMessage.error(id = null, error = ResponseError.parseError("Parse error"))
            val result = dispatcher.dispatch(response)

            result shouldBe null
        }

    @Test
    fun `response for unknown request is ignored`() =
        runTest {
            val response = ResponseMessage.success(id = RequestId.of(999), result = JsonPrimitive("ignored"))
            val result = dispatcher.dispatch(response)

            result shouldBe null
        }

    // ===== Pending Request Management Tests =====

    @Test
    fun `registerPendingRequest creates deferred`() =
        runTest {
            val id = RequestId.of(1)
            val deferred = dispatcher.registerPendingRequest(id)

            deferred.isCompleted shouldBe false
            deferred.isActive shouldBe true
        }

    @Test
    fun `cancelPendingRequest cancels the deferred`() =
        runTest {
            val id = RequestId.of(1)
            val deferred = dispatcher.registerPendingRequest(id)

            dispatcher.cancelPendingRequest(id)

            deferred.isCancelled shouldBe true
        }

    @Test
    fun `cancelPendingRequest for unknown id does nothing`() =
        runTest {
            // Should not throw
            dispatcher.cancelPendingRequest(RequestId.of(999))
        }

    @Test
    fun `multiple pending requests are independent`() =
        runTest {
            val id1 = RequestId.of(1)
            val id2 = RequestId.of(2)

            val deferred1 = dispatcher.registerPendingRequest(id1)
            val deferred2 = dispatcher.registerPendingRequest(id2)

            // Complete only the first one
            val response1 = ResponseMessage.success(id = id1, result = JsonPrimitive("result1"))
            dispatcher.dispatch(response1)

            deferred1.isCompleted shouldBe true
            deferred2.isCompleted shouldBe false

            // Complete the second one
            val response2 = ResponseMessage.success(id = id2, result = JsonPrimitive("result2"))
            dispatcher.dispatch(response2)

            deferred2.isCompleted shouldBe true
            deferred1.await() shouldBe JsonPrimitive("result1")
            deferred2.await() shouldBe JsonPrimitive("result2")
        }

    // ===== JsonRpcException Tests =====

    @Test
    fun `JsonRpcException toResponseError creates correct error`() {
        val exception = JsonRpcException(ResponseError.INVALID_PARAMS, "Invalid params")
        val error = exception.toResponseError()

        error.code shouldBe ResponseError.INVALID_PARAMS
        error.message shouldBe "Invalid params"
        error.data shouldBe null
    }

    @Test
    fun `JsonRpcException toResponseError preserves data`() {
        val data = JsonPrimitive("extra")
        val exception = JsonRpcException(ResponseError.INVALID_PARAMS, "Invalid params", data)
        val error = exception.toResponseError()

        error.data shouldBe data
    }

    @Test
    fun `JsonRpcException factory methodNotFound`() {
        val exception = JsonRpcException.methodNotFound("test/method")
        exception.code shouldBe ResponseError.METHOD_NOT_FOUND
        exception.message shouldBe "Method not found: test/method"
    }

    @Test
    fun `JsonRpcException factory invalidParams`() {
        val exception = JsonRpcException.invalidParams("Expected string")
        exception.code shouldBe ResponseError.INVALID_PARAMS
        exception.message shouldBe "Expected string"
    }

    @Test
    fun `JsonRpcException factory internalError`() {
        val exception = JsonRpcException.internalError("Unexpected error")
        exception.code shouldBe ResponseError.INTERNAL_ERROR
        exception.message shouldBe "Unexpected error"
    }

    // ===== Dispatch Multiple Message Types Tests =====

    @Test
    fun `dispatch handles request message`() =
        runTest {
            dispatcher.onRequest("test/method") { _: kotlinx.serialization.json.JsonElement? ->
                JsonPrimitive("ok")
            }

            val message: Message = RequestMessage(id = RequestId.of(1), method = "test/method")
            val response = dispatcher.dispatch(message)

            response shouldNotBe null
            response.shouldBeInstanceOf<ResponseMessage>()
        }

    @Test
    fun `dispatch handles notification message`() =
        runTest {
            var called = false
            dispatcher.onNotification("test/notify") { _: kotlinx.serialization.json.JsonElement? ->
                called = true
            }

            val message: Message = NotificationMessage(method = "test/notify")
            val response = dispatcher.dispatch(message)

            response shouldBe null
            called shouldBe true
        }

    @Test
    fun `dispatch handles response message`() =
        runTest {
            val id = RequestId.of(1)
            val deferred = dispatcher.registerPendingRequest(id)

            val message: Message = ResponseMessage.success(id = id, result = JsonPrimitive("result"))
            val response = dispatcher.dispatch(message)

            response shouldBe null
            deferred.await() shouldBe JsonPrimitive("result")
        }

    // ===== Handler with Complex JSON Tests =====

    @Test
    fun `handler works with nested JSON objects`() =
        runTest {
            dispatcher.onRequest("test/nested") { params: kotlinx.serialization.json.JsonElement? ->
                val obj = params?.jsonObject
                val nested = obj?.get("nested")?.jsonObject
                val value = nested?.get("value")?.jsonPrimitive?.int
                JsonPrimitive(value?.times(2) ?: 0)
            }

            val params =
                buildJsonObject {
                    put(
                        "nested",
                        buildJsonObject {
                            put("value", JsonPrimitive(21))
                        },
                    )
                }
            val request = RequestMessage(id = RequestId.of(1), method = "test/nested", params = params)
            val response = dispatcher.dispatch(request)

            response!!.result shouldBe JsonPrimitive(42)
        }

    // ===== Concurrent Request Tests =====

    @Test
    fun `dispatcher handles multiple concurrent requests`() =
        runTest {
            dispatcher.onRequest("test/method") { params: kotlinx.serialization.json.JsonElement? ->
                val id = params?.jsonPrimitive?.int ?: 0
                JsonPrimitive(id * 2)
            }

            val request1 = RequestMessage(id = RequestId.of(1), method = "test/method", params = JsonPrimitive(10))
            val request2 = RequestMessage(id = RequestId.of(2), method = "test/method", params = JsonPrimitive(20))
            val request3 = RequestMessage(id = RequestId.of(3), method = "test/method", params = JsonPrimitive(30))

            val response1 = dispatcher.dispatch(request1)
            val response2 = dispatcher.dispatch(request2)
            val response3 = dispatcher.dispatch(request3)

            response1!!.result shouldBe JsonPrimitive(20)
            response2!!.result shouldBe JsonPrimitive(40)
            response3!!.result shouldBe JsonPrimitive(60)
        }
}
