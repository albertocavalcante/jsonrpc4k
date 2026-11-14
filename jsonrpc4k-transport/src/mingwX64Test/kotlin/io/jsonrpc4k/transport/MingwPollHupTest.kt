package io.jsonrpc4k.transport

import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.convert
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.SD_SEND
import platform.posix.closesocket
import platform.posix.shutdown
import platform.windows.ADDRINFOA
import platform.windows.AF_INET
import platform.windows.FILETIME
import platform.windows.GetCurrentProcess
import platform.windows.GetProcessTimes
import platform.windows.SOCK_STREAM
import platform.windows.connect
import platform.windows.freeaddrinfo
import platform.windows.getaddrinfo
import platform.windows.socket
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private val CLOSE_BOUND = 5.seconds
private const val SETTLE_MILLIS = 300L
private const val MEASURE_MILLIS = 1000L

// A spinning wait burns a whole core, about MEASURE_MILLIS of CPU; an idle one burns next to nothing.
private const val CPU_BUDGET_MILLIS = MEASURE_MILLIS / 2

// Far more than loopback send and receive buffers hold, so the send keeps waiting while the peer does not read.
private const val FLOOD_BYTES = 64 * 1024 * 1024

private const val FILETIME_TICKS_PER_MILLI = 10_000L
private const val HIGH_WORD_SHIFT = 32

class MingwPollHupTest {
    // Once the peer has shut down its sending side, WSAPoll reports POLLHUP on every call whatever was requested. A
    // send waiting for buffer space that took POLLHUP as "retry now" spun at full CPU until the peer read or left.
    @Test
    fun `a send waiting on a half closed peer that does not read does not spin`(): Unit =
        runBlocking(Dispatchers.Default) {
            // The send runs outside this scope, so a regression fails here by name instead of hanging.
            val detached = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val listener = TcpListener.bind("127.0.0.1", 0)
            val peer = connectPlainSocket(listener.port)
            var accepted: Transport? = null
            try {
                val transport = withTimeout(TIMEOUT) { listener.accept() }
                accepted = transport
                val sending = detached.async { runCatching { transport.send(ByteArray(FLOOD_BYTES)) } }
                delay(SETTLE_MILLIS)
                assertTrue(sending.isActive, "the send should be waiting for buffer space, but ended with ${outcome(sending)}")
                // The peer sends FIN but keeps its socket open and never reads: WSAPoll now reports POLLHUP.
                shutdown(peer, SD_SEND)
                delay(SETTLE_MILLIS)
                val before = processCpuMillis()
                delay(MEASURE_MILLIS)
                val used = processCpuMillis() - before
                assertTrue(sending.isActive, "the send should still be waiting, but ended with ${outcome(sending)}")
                assertTrue(used < CPU_BUDGET_MILLIS, "the waiting send used $used ms of CPU in $MEASURE_MILLIS ms")
                transport.close()
                val failure = withTimeoutOrNull(CLOSE_BOUND) { sending.await() }?.exceptionOrNull()
                assertTrue(failure is TransportException, "the waiting send ended with $failure")
            } finally {
                accepted?.close()
                closesocket(peer)
                listener.close()
                detached.cancel()
            }
        }
}

@OptIn(ExperimentalCoroutinesApi::class)
private fun outcome(sending: Deferred<Result<Unit>>): String =
    if (sending.isActive) "nothing yet" else sending.getCompleted().fold({ "success" }, { "$it" })

/** User plus kernel CPU time of this process so far, in milliseconds. */
@OptIn(ExperimentalForeignApi::class)
private fun processCpuMillis(): Long =
    memScoped {
        val creation = alloc<FILETIME>()
        val exit = alloc<FILETIME>()
        val kernel = alloc<FILETIME>()
        val user = alloc<FILETIME>()
        check(GetProcessTimes(GetCurrentProcess(), creation.ptr, exit.ptr, kernel.ptr, user.ptr) != 0) {
            "GetProcessTimes failed"
        }
        (kernel.ticks() + user.ticks()) / FILETIME_TICKS_PER_MILLI
    }

@OptIn(ExperimentalForeignApi::class)
private fun FILETIME.ticks(): Long = (dwHighDateTime.toLong() shl HIGH_WORD_SHIFT) or dwLowDateTime.toLong()

/** A plain Winsock socket connected to 127.0.0.1:[port], so the test controls its half-close directly. */
@OptIn(ExperimentalForeignApi::class)
internal fun connectProbeSocket(port: Int): ULong = connectPlainSocket(port)

@OptIn(ExperimentalForeignApi::class)
private fun connectPlainSocket(port: Int): ULong =
    memScoped {
        val hints = alloc<ADDRINFOA>()
        hints.ai_family = AF_INET
        hints.ai_socktype = SOCK_STREAM
        val result = alloc<CPointerVar<ADDRINFOA>>()
        check(getaddrinfo("127.0.0.1", port.toString(), hints.ptr, result.ptr) == 0) { "getaddrinfo failed" }
        val info = checkNotNull(result.value)
        try {
            val handle = socket(info.pointed.ai_family, info.pointed.ai_socktype, info.pointed.ai_protocol)
            check(handle != INVALID_SOCKET_VALUE) { "socket failed" }
            check(connect(handle, info.pointed.ai_addr, info.pointed.ai_addrlen.convert()) == 0) { "connect failed" }
            handle
        } finally {
            freeaddrinfo(info)
        }
    }
