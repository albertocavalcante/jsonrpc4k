package io.jsonrpc4k.transport

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class LspLaunchArgumentsTest {
    @Test
    fun `accepted argument forms`() {
        val table =
            mapOf(
                listOf("--stdio") to LspLaunchArguments(LspTransportSpec.Stdio, null),
                listOf("--node-ipc") to LspLaunchArguments(LspTransportSpec.NodeIpc, null),
                listOf("--socket=6009") to LspLaunchArguments(LspTransportSpec.Socket(6009), null),
                listOf("--socket=65535") to LspLaunchArguments(LspTransportSpec.Socket(65535), null),
                listOf("--pipe=/tmp/x.sock") to LspLaunchArguments(LspTransportSpec.Pipe("/tmp/x.sock"), null),
                listOf("--pipe=\\\\.\\pipe\\lsp-1") to LspLaunchArguments(LspTransportSpec.Pipe("\\\\.\\pipe\\lsp-1"), null),
                listOf("--stdio", "--clientProcessId=123") to LspLaunchArguments(LspTransportSpec.Stdio, 123),
                listOf("--clientProcessId=4294967296", "--socket=6009") to
                    LspLaunchArguments(LspTransportSpec.Socket(6009), 4294967296),
                listOf("--pipe=a=b") to LspLaunchArguments(LspTransportSpec.Pipe("a=b"), null),
            )
        for ((args, expected) in table) {
            assertEquals(expected, LspLaunchArguments.parse(args), "args $args")
        }
    }

    @Test
    fun `rejected argument forms`() {
        val table =
            listOf(
                emptyList<String>(),
                listOf("--clientProcessId=1"),
                listOf("--stdio", "--node-ipc"),
                listOf("--stdio", "--stdio"),
                listOf("--socket=6009", "--pipe=x"),
                listOf("--stdio", "--clientProcessId=1", "--clientProcessId=2"),
                listOf("--stdio=yes"),
                listOf("--node-ipc="),
                listOf("--pipe"),
                listOf("--pipe="),
                listOf("--socket"),
                listOf("--socket="),
                listOf("--socket=0"),
                listOf("--socket=65536"),
                listOf("--socket=-1"),
                listOf("--socket=+80"),
                listOf("--socket=80x"),
                listOf("--socket", "6009"),
                listOf("--stdio", "--clientProcessId"),
                listOf("--stdio", "--clientProcessId="),
                listOf("--stdio", "--clientProcessId=0"),
                listOf("--stdio", "--clientProcessId=-5"),
                listOf("--stdio", "--clientProcessId=abc"),
                listOf("--stdio", "--clientProcessId=99999999999999999999"),
                listOf("--stdio", "--verbose"),
                listOf("--stdio", "positional"),
                listOf("--STDIO"),
                listOf("stdio"),
            )
        for (args in table) {
            assertFailsWith<IllegalArgumentException>("args $args") { LspLaunchArguments.parse(args) }
        }
    }

    @Test
    fun `default transport only fills a missing transport`() {
        assertEquals(
            LspLaunchArguments(LspTransportSpec.Stdio, 7),
            LspLaunchArguments.parse(listOf("--clientProcessId=7"), defaultTransport = LspTransportSpec.Stdio),
        )
        assertEquals(
            LspLaunchArguments(LspTransportSpec.NodeIpc, null),
            LspLaunchArguments.parse(listOf("--node-ipc"), defaultTransport = LspTransportSpec.Stdio),
        )
        assertFailsWith<IllegalArgumentException> {
            LspLaunchArguments.parse(listOf("--bogus"), defaultTransport = LspTransportSpec.Stdio)
        }
    }

    @Test
    fun `pipe names normalize to a Windows pipe path`() {
        assertEquals("\\\\.\\pipe\\lsp", pipePath("lsp"))
        assertEquals("\\\\.\\pipe\\lsp", pipePath("\\\\.\\pipe\\lsp"))
        assertEquals("\\\\.\\pipe\\lsp", pipePath("//./pipe/lsp"))
        for (bad in listOf("", "\\\\.\\pipe\\", "a\\b", "a/b", "\\\\.\\pipe\\a\\b")) {
            assertFailsWith<TransportException>("name '$bad'") { pipePath(bad) }
        }
    }

    @Test
    fun `pipe specs route Windows paths to named pipes`() {
        assertEquals(true, isPipePath("\\\\.\\pipe\\lsp"))
        assertEquals(true, isPipePath("//./pipe/lsp"))
        assertEquals(false, isPipePath("/tmp/x.sock"))
        assertEquals(false, isPipePath("lsp"))
    }
}
