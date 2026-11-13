package io.jsonrpc4k.transport

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.io.RandomAccessFile

private fun isWindows(): Boolean = System.getProperty("os.name").orEmpty().startsWith("Windows", ignoreCase = true)

/**
 * Client-only named pipe support through [RandomAccessFile]. See [NamedPipe.connect] for the
 * half-duplex limitation of the single synchronous handle. Not validated on a Windows runner yet.
 */
internal actual suspend fun connectNamedPipe(pipePath: String): Transport {
    if (!isWindows()) {
        throw UnsupportedTransportException("Named pipes exist only on Windows; $pipePath cannot be opened here")
    }
    return withContext(Dispatchers.IO) {
        try {
            val file = RandomAccessFile(pipePath, "rw")
            StreamTransport(input = PipeInput(file), output = PipeOutput(file))
        } catch (e: IOException) {
            throw TransportException("Failed to connect to named pipe $pipePath", e)
        }
    }
}

internal actual suspend fun bindNamedPipe(pipePath: String): LocalListener =
    throw UnsupportedTransportException(
        "The JVM cannot create named pipe servers without native code (JNI or the foreign function API); " +
            "use TCP or a Unix socket, or run the server on Node.js: $pipePath",
    )

private class PipeInput(
    private val file: RandomAccessFile,
) : InputStream() {
    override fun read(): Int = file.read()

    override fun read(
        b: ByteArray,
        off: Int,
        len: Int,
    ): Int = file.read(b, off, len)

    override fun close() = file.close()
}

private class PipeOutput(
    private val file: RandomAccessFile,
) : OutputStream() {
    override fun write(b: Int) = file.write(b)

    override fun write(
        b: ByteArray,
        off: Int,
        len: Int,
    ) = file.write(b, off, len)

    override fun close() = file.close()
}
