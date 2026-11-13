package io.jsonrpc4k.transport

import kotlin.test.Test
import kotlin.test.assertNotNull

class NodeModulesTest {
    @Test
    fun `lazy Node modules resolve`() {
        listOf("net", "fs", "child_process", "os", "buffer", "process").forEach { name ->
            assertNotNull(nodeRequire(name), name)
        }
    }
}
