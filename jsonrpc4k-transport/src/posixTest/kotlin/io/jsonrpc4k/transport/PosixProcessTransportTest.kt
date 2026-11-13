package io.jsonrpc4k.transport

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private const val SIGTERM_EXIT = 143
private const val SIGKILL_EXIT = 137

class PosixProcessTransportTest {
    @Test
    fun `round trips binary payloads through cat`(): Unit =
        runBlocking(Dispatchers.Default) {
            val transport = ProcessTransport.start(listOf("/bin/cat"))
            try {
                assertTrue(transport.pid > 0)
                // Larger than a pipe buffer, so sending and receiving must overlap; covers every byte value.
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

    @Test
    fun `incoming ends normally after close`(): Unit =
        runBlocking(Dispatchers.Default) {
            val transport = ProcessTransport.start(listOf("/bin/cat"))
            val chunks = async { transport.incoming.toList() }
            transport.close()
            assertEquals(emptyList(), withTimeout(TIMEOUT) { chunks.await() })
            assertEquals(0, transport.awaitExit())
        }

    @Test
    fun `close terminates a child that ignores end of input`(): Unit =
        runBlocking(Dispatchers.Default) {
            val transport = ProcessTransport.start(listOf("/bin/sleep", "60"), exitTimeout = 100.milliseconds)
            withTimeout(TIMEOUT) { transport.close() }
            val code = withTimeout(TIMEOUT) { transport.awaitExit() }
            // The contract is termination; slow runners may need the SIGKILL escalation.
            assertTrue(code == SIGTERM_EXIT || code == SIGKILL_EXIT, "expected exit by SIGTERM or SIGKILL, got $code")
        }

    @Test
    fun `start fails for a missing program`(): Unit =
        runBlocking(Dispatchers.Default) {
            assertFailsWith<TransportException> { ProcessTransport.start(listOf("/nonexistent/jsonrpc4k-program")) }
            assertFailsWith<TransportException> {
                ProcessTransport.start(listOf("/bin/cat"), workingDirectory = "/nonexistent/jsonrpc4k-directory")
            }
            assertFailsWith<TransportException> { ProcessTransport.start(listOf("/bin/cat"), workingDirectory = "/bin/sh") }
            assertFailsWith<TransportException> { ProcessTransport.start(listOf("jsonrpc4k-no-such-program")) }
        }

    @Test
    fun `environment changes reach the child`(): Unit =
        runBlocking(Dispatchers.Default) {
            val transport =
                ProcessTransport.start(
                    listOf("/bin/sh", "-c", "printf '%s|%s' \"\$JSONRPC4K_SET\" \"\${HOME-unset}\""),
                    environment = mapOf("JSONRPC4K_SET" to "value", "HOME" to null),
                )
            try {
                val output = withTimeout(TIMEOUT) { transport.incoming.toList() }.fold("") { acc, c -> acc + c.decodeToString() }
                assertEquals("value|unset", output)
            } finally {
                transport.close()
            }
        }

    @Test
    fun `bare program names are found on PATH and in a changed PATH`(): Unit =
        runBlocking(Dispatchers.Default) {
            val plain = ProcessTransport.start(listOf("cat"))
            val changed = ProcessTransport.start(listOf("cat"), environment = mapOf("PATH" to "/bin:/usr/bin"))
            try {
                assertTrue(plain.pid > 0)
                assertTrue(changed.pid > 0)
            } finally {
                plain.close()
                changed.close()
            }
        }

    @Test
    fun `stderr pipe delivers output and discard drops it`(): Unit =
        runBlocking(Dispatchers.Default) {
            val seen = CompletableDeferred<String>()
            val collected = StringBuilder()
            val piped =
                ProcessTransport.start(
                    listOf("/bin/sh", "-c", "echo oops >&2"),
                    stderr =
                        StderrMode.Pipe { chunk ->
                            collected.append(chunk.decodeToString())
                            if (collected.contains("oops")) seen.complete(collected.toString())
                        },
                )
            val discarded = ProcessTransport.start(listOf("/bin/sh", "-c", "echo oops >&2"), stderr = StderrMode.Discard)
            try {
                assertEquals("oops\n", withTimeout(TIMEOUT) { seen.await() })
                assertEquals(0, withTimeout(TIMEOUT) { discarded.awaitExit() })
            } finally {
                piped.close()
                discarded.close()
            }
        }
}
