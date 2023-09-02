package io.github.rascio.kdataloaders

import io.github.rascio.kdataloaders.CoroutineDataLoaderExecutionScope.Companion.DataLoaderEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.yield
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Represent the request for a certain key of a data loader
 */
data class Query<K : Any, V>(val key: K, val ref: DataLoaderRef<K, V>)
// mimic Pair 'to' constructor
private infix fun <K: Any, V> DataLoaderRef<K, V>.to(key: K) = Query(key, this)

/**
 * Utility listener of execution events
 */
interface DataLoaderEventListener {
    suspend operator fun invoke(event: DataLoaderEvent)

    /**
     * Merge two event listeners creating a new one calling both of them
     */
    operator fun plus(other: DataLoaderEventListener) =
        DataLoaderEventListener { event ->
            this(event)
            other(event)
        }

    companion object {
        /**
         * Constructor mimicking lambda functions
         */
        inline operator fun invoke(crossinline block: suspend (DataLoaderEvent) -> Unit) =
            object : DataLoaderEventListener {
                override suspend fun invoke(event: DataLoaderEvent) = block(event)
            }
    }
}

/**
 * Main scope for data loaders.
 * It contains the required dependencies, and manage the main state
 *
 * When having it in scope (like when using `registry.withDataLoaders { }`)
 * make the DataLoaderRef invokable like a function (using the invoke operator fun)
 *
 * It then will send the query and schedule the dispatch, or join an ongoing dispatch
 * batching together multiple queries
 */
class CoroutineDataLoaderExecutionScope internal constructor(
    private val dataLoaderCoroutineScope: CoroutineScope,
    private val registry: DataLoaderRegistry,
    private val eventListener: DataLoaderEventListener
) : DataLoaderExecutionScope, LogScope {


    private class State {
        /*
         * Contains all the ongoing queries, they are executed asynchronously
         * and then it will be cleared waiting for another dispatch
         */
        private val state = mutableMapOf<DataLoaderRef<*, *>, MutableMap<Any, CompletableDeferred<*>>>()
        // The version will increase every time a query is executed
        val version = AtomicLong()

        /*
         * Append a query in the state, generate a CompletableDeferred for it, if not already available
         */
        fun <K : Any, V> append(query: Query<K, V>): Deferred<V> {
            val (key, ref) = query
            val found = state
                .computeIfAbsent(ref) { mutableMapOf() }
                .computeIfAbsent(key) { CompletableDeferred<Any>() }

            //version.incrementAndGet()
            @Suppress("UNCHECKED_CAST")
            return found as Deferred<V>
        }

        /*
         * Represent a batch of queries that needs to be executed
         */
        data class QueryBatch<K : Any, V>(val ref: DataLoaderRef<K, V>, val entries: Map<K, CompletableDeferred<V>>)
        @Suppress("UNCHECKED_CAST")
        private fun <K : Any, V> getBatch(ref: DataLoaderRef<K, V>): QueryBatch<K, V> =
            state.computeIfAbsent(ref) { ConcurrentHashMap<Any, CompletableDeferred<*>>() }
                .entries
                .associate { (k, v) -> k as K to v as CompletableDeferred<V> }
                .let { QueryBatch(ref, it) }
        // Virtual property returning the batches of queries that need to be dispatched
        val queriesToDispatch: List<QueryBatch<*, *>> get() {
            return state.keys
                .map { getBatch(it) }
        }

        fun clear() {
            state.clear()
        }
    }

    /*
     * Used to synchronize access over the State
     */
    private val mutex = Mutex()
    private val queue: State = State()
    private val isDispatching = AtomicBoolean(false)

    /**
     * Run the query during the next dispatch.
     */
    override suspend fun <K : Any, V> DataLoaderRef<K, V>.invoke(key: K): Deferred<V> {
        val query = this to key
        eventListener(DataLoaderEvent.AcquiringLock(query))
        queue.version.incrementAndGet()
        return mutex.withLock {
            queue.append(query).also {
                eventListener(DataLoaderEvent.QueryAppended(query))
                // If not already dispatching, do the dispatch
                if (!isDispatching.get()) {
                    dispatch(query)
                }
            }
        }
    }


    private suspend fun <K : Any, V> dispatch(query: Query<K, V>) {
        dataLoaderCoroutineScope.launch(CoroutineName("Dispatch-${query.ref::class.simpleName}-${query.key}")) {
            eventListener(DataLoaderEvent.DispatchRequested(query))
            // Check no other coroutines are dispatching at the moment
            if (isDispatching.compareAndSet(false, true)) {
                eventListener(DataLoaderEvent.DispatchAccepted(query))
                awaitConcurrentDataLoadersAppend(query)
                mutex.withLock {
                    // We got the lock, no one can append any other query
                    eventListener(DataLoaderEvent.DispatchStarted(query))
                    try {
                        // Get all the batches and dispatch them asynchronously
                        queue.queriesToDispatch
                            .forEach { batch -> dispatchAsync(batch) }

                        eventListener(DataLoaderEvent.DispatchCompleted(query))
                        // all the batches have been dispatched
                        // clear the queue and release everything
                        // the scope is now ready to collect new queries
                        queue.clear()
                    } finally {
                        isDispatching.set(false)
                    }
                }
            } else {
                eventListener(DataLoaderEvent.DispatchRejected(query))
            }
        }
    }

    private suspend fun awaitConcurrentDataLoadersAppend(query: Query<*, *>) {
        var read: Long = queue.version.get()
        var expected: Long
        do {
            eventListener(DataLoaderEvent.WaitForBatching(query, read))
            expected = read
            // try to run other coroutines if scheduled
            // yield()
            // a small delay works better than yield() as it allows to aggregate on multiple threads
            delay(5)
            read = queue.version.get()
            eventListener(DataLoaderEvent.CheckForBatching(query, expected, read))
            // if yield() did run a coroutine which has appended a query
            // the version will be different
        } while (read != expected)
    }


    private suspend fun <K: Any, V> dispatchAsync(batch: State.QueryBatch<K, V>) = dataLoaderCoroutineScope.launch {
        dispatch(batch.ref, batch.entries)
    }

    private suspend fun <K : Any, V> dispatch(
        ref: DataLoaderRef<K, V>,
        values: Map<K, CompletableDeferred<V>>
    ) {
        try {
            val dataLoader = registry[ref]
            eventListener(DataLoaderEvent.RefDispatchStarted(ref))
            // execute the queries batched
            val results = dataLoader.load(values.keys)
            values.forEach { (k, out) ->
                // publish their results
                // in case we are missing a result for a query
                // its CompletableDeferred is resolved with an exception
                results[k]?.also { out.complete(it) }
                    ?: out.completeExceptionally(MissingResultException(k, ref))
            }
            eventListener(DataLoaderEvent.RefDispatchSucceed(ref, results.keys))

        } catch (e: Exception) {
            values.values.forEach { it.completeExceptionally(e) }
            eventListener(DataLoaderEvent.RefDispatchFailed(ref, e))
        }
    }

    companion object {

        sealed interface DataLoaderEvent {
            data class AcquiringLock(val query: Query<*, *>) : DataLoaderEvent
            data class QueryAppended(val query: Query<*, *>) : DataLoaderEvent
            data class DispatchRequested(val query: Query<*, *>) : DataLoaderEvent
            data class DispatchStarted(val query: Query<*, *>) : DataLoaderEvent
            data class DispatchCompleted(val query: Query<*, *>) : DataLoaderEvent
            data class DispatchAccepted(val query: Query<*, *>) : DataLoaderEvent
            data class DispatchRejected(val query: Query<*, *>) : DataLoaderEvent
            data class WaitForBatching(val query: Query<*, *>, val version: Any) : DataLoaderEvent
            data class CheckForBatching(val query: Query<*, *>, val expected: Any, val actual: Any) : DataLoaderEvent
            data class RefDispatchStarted(val ref: DataLoaderRef<*, *>) : DataLoaderEvent
            data class RefDispatchFailed(val ref: DataLoaderRef<*, *>, val e: Exception) : DataLoaderEvent
            data class RefDispatchSucceed(val ref: DataLoaderRef<*, *>, val keys: Set<*>) : DataLoaderEvent
        }
    }
}
