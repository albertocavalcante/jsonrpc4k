package io.jsonrpc4k.transport

import org.khronos.webgl.Uint8Array
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertTrue

private const val BYTE_VALUES = 256
private const val SLICE_START = 3
private const val SLICE_END = 9
private const val UTF8_TEXT = "héllo € 𝄞 日本"

/**
 * Buffer <-> ByteArray conversion used by the Node stdio and socket transports.
 *
 * Node-only: the transports require Node's `buffer` module, so these tests return early under a browser runner.
 */
class BufferConversionTest {
    @Test
    fun `every byte value round-trips through Buffer and ByteArray`() {
        if (!isNode()) return
        val source = BufferModule.Buffer.alloc(BYTE_VALUES)
        for (i in 0 until BYTE_VALUES) {
            source[i] = i
        }

        val bytes = bufferToByteArray(source.unsafeCast<Uint8Array>())
        assertContentEquals(ByteArray(BYTE_VALUES) { it.toByte() }, bytes)

        val back = byteArrayToBuffer(bytes)
        assertTrue(BufferModule.Buffer.isBuffer(back).unsafeCast<Boolean>())
        assertEquals(0, BufferModule.Buffer.compare(source, back).unsafeCast<Int>())
    }

    @Test
    fun `multi-byte UTF-8 text round-trips exactly`() {
        if (!isNode()) return
        val source = BufferModule.Buffer.from(UTF8_TEXT, "utf8")

        val bytes = bufferToByteArray(source.unsafeCast<Uint8Array>())
        assertContentEquals(UTF8_TEXT.encodeToByteArray(), bytes)

        val back = byteArrayToBuffer(bytes)
        assertEquals(UTF8_TEXT, back.toString("utf8").unsafeCast<String>())
    }

    @Test
    fun `a pooled slice is copied from its offset without aliasing`() {
        if (!isNode()) return
        val pool = BufferModule.Buffer.alloc(BYTE_VALUES)
        for (i in 0 until BYTE_VALUES) {
            pool[i] = i
        }
        val slice = pool.subarray(SLICE_START, SLICE_END)

        val bytes = bufferToByteArray(slice.unsafeCast<Uint8Array>())
        assertContentEquals(ByteArray(SLICE_END - SLICE_START) { (SLICE_START + it).toByte() }, bytes)

        pool[SLICE_START] = 0
        assertEquals(SLICE_START.toByte(), bytes[0])
    }
}

private fun isNode(): Boolean =
    js("typeof process !== 'undefined' && process.versions != null && process.versions.node != null")
        .unsafeCast<Boolean>()
