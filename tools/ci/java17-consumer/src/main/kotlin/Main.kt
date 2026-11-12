import io.jsonrpc4k.core.Connection
import io.jsonrpc4k.core.JsonRpcCodec
import io.jsonrpc4k.core.RequestId
import io.jsonrpc4k.core.ResponseMessage
import io.jsonrpc4k.transport.Transport
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonElement

fun main() =
    runBlocking {
        check(System.getProperty("java.specification.version") == "17")
        check(Transport::class.java.isInterface)
        Class.forName("kotlinx.collections.immutable.ExtensionsKt")
        val codec = JsonRpcCodec.Default
        val connection = Connection()
        val response = async(start = CoroutineStart.UNDISPATCHED) { connection.request<JsonElement>("test", null) }
        val outgoing = connection.outgoing.first().decodeToString()
        check(outgoing.contains("\"method\":\"test\""))
        connection.receive(codec.encode(ResponseMessage.success(RequestId.of(1), null)))
        check(response.await() == null)
        connection.close()
        println("Java 17 artifact consumer passed")
    }
