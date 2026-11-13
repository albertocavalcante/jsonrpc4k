package io.jsonrpc4k.transport

import kotlinx.cinterop.CPointer
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.convert
import platform.posix.pollfd

@OptIn(ExperimentalForeignApi::class)
internal actual fun pollOne(
    entry: CPointer<pollfd>,
    timeoutMillis: Int,
): Int = platform.posix.poll(entry, 1.convert(), timeoutMillis)
