package io.jsonrpc4k.transport

private const val SUFFIX_RANGE = 1_000_000_000

internal fun isWindowsHost(): Boolean = js("process.platform === 'win32'").unsafeCast<Boolean>()

internal fun randomSuffix(): Long = (js("Math.random()").unsafeCast<Double>() * SUFFIX_RANGE).toLong()
