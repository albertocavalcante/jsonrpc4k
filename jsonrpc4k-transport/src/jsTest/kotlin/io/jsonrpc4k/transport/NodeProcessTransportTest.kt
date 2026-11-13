package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private const val SIGTERM_EXIT = 143
private const val ECHO = "process.stdin.pipe(process.stdout)"
private const val ENV_SCRIPT = "process.stdout.write(process.env.JSONRPC4K_SET + '|' + String(process.env.HOME)); console.error('oops')"

private fun node(script: String): List<String> = listOf(NodeProcess.execPath, "-e", script)

private fun List<ByteArray>.text(): String = fold("") { acc, chunk -> acc + chunk.decodeToString() }

// runTest skips delays in virtual time; real child processes need the real clock, hence Dispatchers.Default.
class NodeProcessTransportTest {
    @Test
    fun `round trips binary payloads through a node child`() =
        runTest(timeout = TIMEOUT) {
            withContext(Dispatchers.Default) {
                val transport = ProcessTransport.start(node(ECHO))
                try {
                    // Larger than a pipe buffer; covers every byte value.
                    val payload = ByteArray(256 * 1024) { it.toByte() }
                    val received =
                        async {
                            var bytes = ByteArray(0)
                            transport.incoming.first {
                                bytes += it
                                bytes.size >= payload.size
                            }
                            bytes
                        }
                    transport.send(payload)
                    assertContentEquals(payload, withTimeout(TIMEOUT) { received.await() })
                    transport.close()
                    assertEquals(0, withTimeout(TIMEOUT) { transport.awaitExit() })
                    assertFalse(transport.isConnected)
                    assertFailsWith<TransportException> { transport.send(byteArrayOf(1)) }
                } finally {
                    transport.close()
                }
            }
        }

    @Test
    fun `incoming ends normally after close`() =
        runTest(timeout = TIMEOUT) {
            withContext(Dispatchers.Default) {
                val transport = ProcessTransport.start(node(ECHO))
                val chunks = async { transport.incoming.toList() }
                transport.close()
                assertEquals(emptyList(), withTimeout(TIMEOUT) { chunks.await() })
            }
        }

    @Test
    fun `incoming collected after close ends`() =
        runTest(timeout = TIMEOUT) {
            withContext(Dispatchers.Default) {
                val transport = ProcessTransport.start(node(ECHO))
                transport.close()
                assertEquals(emptyList(), withTimeout(TIMEOUT) { transport.incoming.toList() })
            }
        }

    @Test
    fun `close terminates a child that ignores end of input`() =
        runTest(timeout = TIMEOUT) {
            withContext(Dispatchers.Default) {
                val transport = ProcessTransport.start(node("setInterval(() => {}, 1000)"), exitTimeout = 100.milliseconds)
                withTimeout(TIMEOUT) { transport.close() }
                assertEquals(SIGTERM_EXIT, withTimeout(TIMEOUT) { transport.awaitExit() })
            }
        }

    @Test
    fun `start fails for a missing program`() =
        runTest(timeout = TIMEOUT) {
            withContext(Dispatchers.Default) {
                assertFailsWith<TransportException> { ProcessTransport.start(listOf("/nonexistent/jsonrpc4k-program")) }
            }
        }

    @Test
    fun `environment changes and stderr reach the host`() =
        runTest(timeout = TIMEOUT) {
            withContext(Dispatchers.Default) {
                val stderr = StringBuilder()
                val seen = CompletableDeferred<Unit>()
                val transport =
                    ProcessTransport.start(
                        node(ENV_SCRIPT),
                        environment = mapOf("JSONRPC4K_SET" to "value", "HOME" to null),
                        stderr =
                            StderrMode.Pipe {
                                stderr.append(it.decodeToString())
                                if (stderr.contains("oops\n")) seen.complete(Unit)
                            },
                    )
                try {
                    assertEquals("value|undefined", withTimeout(TIMEOUT) { transport.incoming.toList() }.text())
                    withTimeout(TIMEOUT) { seen.await() }
                    assertEquals("oops\n", stderr.toString())
                } finally {
                    transport.close()
                }
            }
        }
}
