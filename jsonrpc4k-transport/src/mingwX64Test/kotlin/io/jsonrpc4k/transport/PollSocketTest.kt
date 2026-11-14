package io.jsonrpc4k.transport

import kotlinx.cinterop.ExperimentalForeignApi
import platform.posix.POLLRDNORM
import platform.posix.WSAEINTR
import platform.posix.WSAENETDOWN
import platform.posix.WSASetLastError
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.TimeSource

private const val SLICE_MILLIS = 100

@OptIn(ExperimentalForeignApi::class)
class PollSocketTest {
    // A WSAPoll failure used to sleep out the slice and report "not ready", so a lasting failure idled forever at ten
    // wake-ups per second instead of failing the read or write.
    @Test
    fun `a WSAPoll failure throws instead of idling`() {
        var calls = 0
        val failure =
            assertFailsWith<TransportException> {
                pollSocket(INVALID_SOCKET_VALUE, POLLRDNORM, POLLRDNORM, SLICE_MILLIS) { _, _ ->
                    calls++
                    WSASetLastError(WSAENETDOWN)
                    SOCKET_ERROR_VALUE
                }
            }
        assertEquals(1, calls)
        assertTrue("$WSAENETDOWN" in failure.message.orEmpty(), "unexpected message: ${failure.message}")
    }

    @Test
    fun `an interrupted WSAPoll sleeps out the slice and reports not ready`() {
        val start = TimeSource.Monotonic.markNow()
        val ready =
            pollSocket(INVALID_SOCKET_VALUE, POLLRDNORM, POLLRDNORM, SLICE_MILLIS) { _, _ ->
                WSASetLastError(WSAEINTR)
                SOCKET_ERROR_VALUE
            }
        assertFalse(ready)
        // Windows timers tick every 15.6 ms or so; half a slice proves the wait slept rather than returned at once.
        val elapsed = start.elapsedNow()
        assertTrue(elapsed >= (SLICE_MILLIS / 2).milliseconds, "returned after $elapsed")
    }
}
