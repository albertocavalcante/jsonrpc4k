package io.jsonrpc4k.transport

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

private const val FLOOD_BYTE: Byte = 0x65
private const val FLOOD_BYTES = 4L * 1024 * 1024
private const val READY = "ready\n"
private val TIMEOUT = 30.seconds
private val CLOSE_BOUND = 5.seconds

/** Variables that make a child JVM print notices on stderr. */
private val QUIET_JVM: Map<String, String?> = mapOf("JDK_JAVA_OPTIONS" to null, "JAVA_TOOL_OPTIONS" to null, "_JAVA_OPTIONS" to null)

class ProcessTransportTest {
    @Test
    fun `round trips binary payloads through a child process`() =
        runBlocking {
            val transport = startFixture("echo")
            try {
                assertTrue(transport.pid > 0)
                val received = Received(this, transport)
                // Larger than a pipe buffer, so sending and receiving must overlap; covers every byte value.
                val payload = ByteArray(256 * 1024) { it.toByte() }
                transport.send(payload)
                assertContentEquals(payload, received.awaitSize(payload.size))
                transport.close()
                assertEquals(0, withTimeout(TIMEOUT) { transport.awaitExit() })
                assertContentEquals(payload, received.awaitEnd())
                assertFailsWith<TransportException> { transport.send(byteArrayOf(1)) }
                assertFalse(transport.isConnected)
            } finally {
                transport.close()
            }
        }

    @Test
    fun `incoming ends normally when the child exits during a read`() =
        runBlocking {
            val transport = startFixture("exit")
            try {
                val received = Received(this, transport)
                transport.send(byteArrayOf(1))
                assertEquals("bye", received.awaitEnd().decodeToString())
                assertEquals(3, withTimeout(TIMEOUT) { transport.awaitExit() })
                assertFalse(transport.isConnected)
            } finally {
                transport.close()
            }
        }

    @Test
    fun `close ends a child that keeps stdout open`() =
        runBlocking {
            assertCloseEnds(startFixture("hold", exitTimeout = 2.seconds))
        }

    @Test
    fun `close kills a child that ignores termination`() =
        runBlocking {
            assertCloseEnds(startFixture("stubborn", exitTimeout = 1.seconds))
        }

    @Test
    fun `a stderr flood is drained to the callback without stalling stdout`() =
        runBlocking {
            val flooded = AtomicLong()
            val stderr = StderrMode.Pipe { chunk -> flooded.addAndGet(chunk.count { it == FLOOD_BYTE }.toLong()) }
            val transport = startFixture("flood", stderr = stderr)
            try {
                assertEquals("done\n", Received(this, transport).awaitEnd().decodeToString())
                assertEquals(0, withTimeout(TIMEOUT) { transport.awaitExit() })
                // The drain thread may still be delivering the tail of the pipe after the child exits.
                withTimeout(TIMEOUT) { while (flooded.get() < FLOOD_BYTES) delay(10) }
            } finally {
                transport.close()
            }
        }

    @Test
    fun `a discarded stderr flood does not stall the child`() =
        runBlocking {
            val transport = startFixture("flood", stderr = StderrMode.Discard)
            try {
                assertEquals("done\n", Received(this, transport).awaitEnd().decodeToString())
                assertEquals(0, withTimeout(TIMEOUT) { transport.awaitExit() })
            } finally {
                transport.close()
            }
        }

    @Test
    fun `the child gets the requested environment and working directory`() =
        runBlocking {
            val directory = Files.createTempDirectory("jsonrpc4k-cwd-")
            try {
                val environment = mapOf("JSONRPC4K_FIXTURE" to "fixture-value", "PATH" to null)
                val transport = startFixture("info", environment = environment, workingDirectory = directory.toString())
                try {
                    val lines = Received(this, transport).awaitEnd().decodeToString().lines()
                    assertEquals("fixture-value", lines[0])
                    assertEquals("absent", lines[1])
                    assertEquals(directory.toRealPath(), Path.of(lines[2]).toRealPath())
                } finally {
                    transport.close()
                }
            } finally {
                Files.deleteIfExists(directory)
            }
        }

    @Test
    fun `a program that cannot start fails with a transport exception`() =
        runBlocking {
            val missing = File(System.getProperty("java.io.tmpdir"), "jsonrpc4k-missing-${System.nanoTime()}").path
            assertFailsWith<IllegalArgumentException> { ProcessTransport.start(emptyList()) }
            val failure = assertFailsWith<TransportException> { ProcessTransport.start(listOf(missing)) }
            assertTrue(failure.cause is IOException)
        }

    /** Closes a child that stays alive after stdin EOF and checks that close is bounded and ends it. */
    private suspend fun CoroutineScope.assertCloseEnds(transport: ProcessTransport) {
        try {
            val received = Received(this, transport)
            assertEquals(READY, received.awaitSize(READY.length).decodeToString())
            val elapsed = measureTime { transport.close() }
            assertTrue(elapsed < CLOSE_BOUND, "close took $elapsed")
            received.awaitEnd()
            assertNotEquals(0, withTimeout(TIMEOUT) { transport.awaitExit() })
        } finally {
            transport.close()
        }
    }

    private suspend fun startFixture(
        mode: String,
        stderr: StderrMode = StderrMode.Inherit,
        environment: Map<String, String?> = emptyMap(),
        workingDirectory: String? = null,
        exitTimeout: Duration = 2.seconds,
    ): ProcessTransport =
        ProcessTransport.start(
            command = listOf(javaExecutable, "-Xmx128m", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", fixtureSource, mode),
            environment = QUIET_JVM + environment,
            workingDirectory = workingDirectory,
            stderr = stderr,
            exitTimeout = exitTimeout,
        )

    private companion object {
        val javaExecutable: String = Path.of(System.getProperty("java.home"), "bin", "java").toString()

        /** Fixture source copied once to a temporary directory, for the single-file source launcher. */
        val fixtureSource: String by lazy {
            val directory = Files.createTempDirectory("jsonrpc4k-process-")
            val source = directory.resolve("ProcessTransportFixture.java")
            val resource = checkNotNull(ProcessTransportTest::class.java.getResourceAsStream("/ProcessTransportFixture.java"))
            resource.use { Files.copy(it, source) }
            directory.toFile().deleteOnExit()
            source.toFile().deleteOnExit()
            source.toString()
        }
    }
}

/** Collects [Transport.incoming] in the background until it ends. */
private class Received(
    scope: CoroutineScope,
    transport: Transport,
) {
    private val bytes = MutableStateFlow(ByteArray(0))
    private val reader: Job =
        scope.launch(Dispatchers.IO) {
            transport.incoming.collect { chunk -> bytes.value += chunk }
        }

    suspend fun awaitSize(size: Int): ByteArray = withTimeout(TIMEOUT) { bytes.first { it.size >= size } }

    suspend fun awaitEnd(): ByteArray {
        withTimeout(TIMEOUT) { reader.join() }
        return bytes.value
    }
}
