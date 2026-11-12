package io.jsonrpc4k.core

import kotlinx.collections.immutable.PersistentSet
import kotlinx.collections.immutable.persistentHashSetOf
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.MutableStateFlow

internal class OutgoingDrain {
    private data class State(
        val collecting: Boolean = false,
        val failure: Throwable? = null,
        val barriers: PersistentSet<CompletableDeferred<Unit>> = persistentHashSetOf(),
    )

    private val state = MutableStateFlow(State())
    val pendingCount: Int get() = state.value.barriers.size

    fun acquireCollector() {
        while (true) {
            val current = state.value
            check(!current.collecting) { "Outgoing messages already have a collector" }
            if (state.compareAndSet(current, current.copy(collecting = true))) return
        }
    }

    fun releaseCollector() {
        while (true) {
            val current = state.value
            if (state.compareAndSet(current, current.copy(collecting = false))) return
        }
    }

    fun register(barrier: CompletableDeferred<Unit>) {
        while (true) {
            val current = state.value
            current.failure?.let { throw it }
            if (state.compareAndSet(current, current.copy(barriers = current.barriers.add(barrier)))) return
        }
    }

    fun unregister(barrier: CompletableDeferred<Unit>) {
        while (true) {
            val current = state.value
            if (state.compareAndSet(current, current.copy(barriers = current.barriers.remove(barrier)))) return
        }
    }

    fun fail(cause: Throwable) {
        val error = IllegalStateException("Outgoing delivery was interrupted", cause)
        while (true) {
            val current = state.value
            if (current.failure != null) return
            val next = current.copy(failure = error, barriers = persistentHashSetOf())
            if (state.compareAndSet(current, next)) {
                current.barriers.forEach { it.completeExceptionally(error) }
                return
            }
        }
    }
}
