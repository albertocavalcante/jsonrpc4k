package io.jsonrpc4k.transport

import kotlinx.cinterop.CPointerVar
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.alloc
import kotlinx.cinterop.memScoped
import kotlinx.cinterop.pointed
import kotlinx.cinterop.ptr
import kotlinx.cinterop.value
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import platform.posix.AF_INET
import platform.posix.SHUT_WR
import platform.posix.SOCK_STREAM
import platform.posix.addrinfo
import platform.posix.close
import platform.posix.connect
import platform.posix.freeaddrinfo
import platform.posix.getaddrinfo
import platform.posix.shutdown
import platform.posix.socket
import kotlin.test.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

private val TIMEOUT = 30.seconds
private val CLOSE_BOUND = 5.seconds
private const val SETTLE_MILLIS = 300L

// Far more than loopback send and receive buffers hold, so the send keeps waiting while the peer does not read.
private const val FLOOD_BYTES = 32 * 1024 * 1024

class PosixSocketCloseTest {
    // Once the peer's FIN has arrived, macOS fails shutdown(SHUT_RDWR) with ENOTCONN without shutting the write side
    // down, so a close that relied on it to wake a send blocked on a full buffer waited forever.
    @Test
    fun `close returns while a send waits on a half closed peer that does not read`(): Unit =
        runBlocking(Dispatchers.Default) {
            // The send and the close run outside this scope, so a regression fails here by name instead of hanging.
            val detached = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val listener = TcpListener.bind("127.0.0.1", 0)
            val peer = connectPlainSocket(listener.port)
            var accepted: Transport? = null
            try {
                val transport = withTimeout(TIMEOUT) { listener.accept() }
                accepted = transport
                val sending = detached.async { runCatching { transport.send(ByteArray(FLOOD_BYTES)) } }
                delay(SETTLE_MILLIS)
                assertTrue(sending.isActive, "the send should be waiting for buffer space")
                // The peer sends FIN but keeps its socket open and never reads.
                shutdown(peer, SHUT_WR)
                delay(SETTLE_MILLIS)
                val closing = detached.launch { transport.close() }
                val closed = withTimeoutOrNull(CLOSE_BOUND) { closing.join() } != null
                assertTrue(closed, "close() did not return within $CLOSE_BOUND")
                val failure = withTimeoutOrNull(CLOSE_BOUND) { sending.await() }?.exceptionOrNull()
                assertTrue(failure is TransportException, "the waiting send ended with $failure")
            } finally {
                // Releases the socket and the waiting send even when an assertion above failed.
                accepted?.close()
                close(peer)
                listener.close()
                detached.cancel()
            }
        }
}

/** A plain socket connected to 127.0.0.1:[port], so the test controls its half-close directly. */
@OptIn(ExperimentalForeignApi::class)
private fun connectPlainSocket(port: Int): Int =
    memScoped {
        val hints = alloc<addrinfo>()
        hints.ai_family = AF_INET
        hints.ai_socktype = SOCK_STREAM
        val result = alloc<CPointerVar<addrinfo>>()
        check(getaddrinfo("127.0.0.1", port.toString(), hints.ptr, result.ptr) == 0) { "getaddrinfo failed" }
        val info = checkNotNull(result.value)
        try {
            val fd = socket(info.pointed.ai_family, info.pointed.ai_socktype, info.pointed.ai_protocol)
            check(fd >= 0) { "socket failed" }
            check(connect(fd, info.pointed.ai_addr, info.pointed.ai_addrlen) == 0) { "connect failed" }
            fd
        } finally {
            freeaddrinfo(info)
        }
    }
