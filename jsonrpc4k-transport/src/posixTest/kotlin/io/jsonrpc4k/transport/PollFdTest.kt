package io.jsonrpc4k.transport

import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.convert
import platform.posix.EBADF
import platform.posix.EINTR
import platform.posix.POLLIN
import platform.posix.set_posix_errno
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

private const val SLICE_MILLIS = 10
private const val ANY_FD = 0

@OptIn(ExperimentalForeignApi::class)
class PollFdTest {
    // A retried failure that is not EINTR made a read loop spin on poll() without ever sleeping.
    @Test
    fun `a poll failure other than EINTR throws`() {
        var calls = 0
        val failure =
            assertFailsWith<PollFailedException> {
                pollFd(ANY_FD, POLLIN.convert(), SLICE_MILLIS) { _, _ ->
                    calls++
                    set_posix_errno(EBADF)
                    -1
                }
            }
        assertEquals(EBADF, failure.errno)
        assertEquals(1, calls)
    }

    @Test
    fun `an interrupted poll reports not ready so the caller retries`() {
        val ready =
            pollFd(ANY_FD, POLLIN.convert(), SLICE_MILLIS) { _, _ ->
                set_posix_errno(EINTR)
                -1
            }
        assertFalse(ready)
    }
}
