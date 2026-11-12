package io.jsonrpc4k.core

/** Counts structural delimiters outside strings and configured comments before tree decoding. */
internal class JsonNestingGuard(
    private val maximum: Int,
    private val allowComments: Boolean,
) {
    private var depth = 0
    private var quoted = false
    private var escaped = false
    private var lineComment = false
    private var blockComment = false
    private var bare = false

    fun check(bytes: ByteArray) {
        var index = 0
        while (index < bytes.size) {
            val current = bytes[index].toInt().toChar()
            val next = if (index + 1 < bytes.size) bytes[index + 1].toInt().toChar() else '\u0000'
            if (consume(current, next)) index++
            index++
        }
    }

    private fun consume(
        current: Char,
        next: Char,
    ): Boolean {
        if (lineComment) {
            if (current == '\n') lineComment = false
            return false
        }
        if (blockComment) {
            if (current == '*' && next == '/') {
                blockComment = false
                return true
            }
            return false
        }
        if (quoted) {
            consumeQuoted(current)
            return false
        }
        return consumeOutside(current, next)
    }

    private fun consumeQuoted(current: Char) {
        when {
            escaped -> escaped = false
            current == '\\' -> escaped = true
            current == '"' -> quoted = false
        }
    }

    private fun consumeOutside(
        current: Char,
        next: Char,
    ): Boolean {
        when (current) {
            '"' -> {
                quoted = true
                bare = false
            }
            '/' -> return consumeSlash(next)
            '{', '[' -> {
                bare = false
                depth++
                if (depth > maximum) throw JsonRpcProtocolException("JSON nesting exceeds $maximum")
            }
            '}', ']' -> {
                bare = false
                if (depth > 0) depth--
            }
            ',', ':', ' ', '\t', '\n', '\r' -> bare = false
            else -> bare = true
        }
        return false
    }

    private fun consumeSlash(next: Char): Boolean {
        if (allowComments && !bare) {
            when (next) {
                '/' -> {
                    lineComment = true
                    return true
                }
                '*' -> {
                    blockComment = true
                    return true
                }
            }
        }
        bare = true
        return false
    }
}
