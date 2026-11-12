package io.jsonrpc4k.core

/**
 * Escapes lone UTF-16 units inside quoted JSON strings before conversion to UTF-8.
 * This is a lexical pass, not JSON validation. It preserves quoted string values in valid JSON
 * and configured comment-bearing JSON. It does not repair malformed raw output or preserve
 * its original spelling. Text outside quoted strings uses ordinary UTF-8 conversion.
 * [maxCharacters] is a lower-bound byte check; callers must still check the final frame size.
 */
internal fun String.escapeLoneJsonSurrogates(
    maxCharacters: Int = Int.MAX_VALUE,
    allowComments: Boolean = false,
): String {
    if (length > maxCharacters) throw JsonRpcProtocolException("Encoded JSON exceeds wire byte limit")
    var characters = length
    var needsEscaping = false
    forEachLoneJsonSurrogate(allowComments) { _, _ ->
        if (characters > maxCharacters - ESCAPE_EXPANSION) {
            throw JsonRpcProtocolException("Encoded JSON exceeds wire byte limit")
        }
        characters += ESCAPE_EXPANSION
        needsEscaping = true
    }
    if (!needsEscaping) return this

    val escaped = StringBuilder(characters)
    var copiedUntil = 0
    forEachLoneJsonSurrogate(allowComments) { index, character ->
        escaped.append(this, copiedUntil, index)
        escaped.append("\\u")
        for (shift in HIGH_HEX_SHIFT downTo 0 step HEX_BITS) {
            escaped.append(HEX_DIGITS[(character.code ushr shift) and HEX_MASK])
        }
        copiedUntil = index + 1
    }
    escaped.append(this, copiedUntil, length)
    return escaped.toString()
}

private fun String.forEachLoneJsonSurrogate(
    allowComments: Boolean,
    action: (Int, Char) -> Unit,
) {
    JsonStringScanner(this, allowComments).visit(action)
}

private class JsonStringScanner(
    private val text: String,
    private val allowComments: Boolean,
) {
    private var quoted = false
    private var escaped = false
    private var bare = false
    private var index = 0

    fun visit(action: (Int, Char) -> Unit) {
        while (index < text.length) {
            val character = text[index]
            if (quoted) consumeQuoted(character, action) else consumeOutside(character)
            index++
        }
    }

    private fun consumeQuoted(
        character: Char,
        action: (Int, Char) -> Unit,
    ) {
        when {
            escaped -> escaped = false
            character == '\\' -> escaped = true
            character == '"' -> quoted = false
            character in '\uD800'..'\uDBFF' -> {
                if (index + 1 < text.length && text[index + 1] in '\uDC00'..'\uDFFF') {
                    index++
                } else {
                    action(index, character)
                }
            }
            character in '\uDC00'..'\uDFFF' -> action(index, character)
        }
    }

    private fun consumeOutside(character: Char) {
        when (character) {
            '"' -> {
                quoted = true
                bare = false
            }
            '/' -> consumeSlash()
            '{', '[', '}', ']', ',', ':', ' ', '\t', '\n', '\r' -> bare = false
            else -> bare = true
        }
    }

    private fun consumeSlash() {
        val end = if (allowComments && !bare) commentEnd() else index
        if (end != index) index = end - 1 else bare = true
    }

    private fun commentEnd(): Int =
        when (text.getOrNull(index + 1)) {
            '/' -> text.indexOf('\n', index + COMMENT_TOKEN_LENGTH).let { if (it < 0) text.length else it + 1 }
            '*' -> text.indexOf("*/", index + COMMENT_TOKEN_LENGTH).let { if (it < 0) text.length else it + COMMENT_TOKEN_LENGTH }
            else -> index
        }
}

private const val ESCAPE_EXPANSION = 5
private const val HIGH_HEX_SHIFT = 12
private const val HEX_BITS = 4
private const val HEX_MASK = 15
private const val HEX_DIGITS = "0123456789abcdef"

private const val COMMENT_TOKEN_LENGTH = 2
