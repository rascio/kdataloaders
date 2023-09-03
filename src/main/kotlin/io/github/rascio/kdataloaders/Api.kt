package io.github.rascio.kdataloaders

import kotlinx.coroutines.Deferred
import kotlin.coroutines.CoroutineContext

/**
 * Represent a DataLoader.
 * When working in a DataLoaderExecutionScope this can be used as a function to trigger the data loader.
 * It's good practice to have them as object, like:
 * ```
 * object GetSomethingById : DataLoaderRef<UUID, Something>
 * ```
 * @param K the key to load the data
 * @param V the data to retrieve
 */
interface DataLoaderRef<K : Any, V> {
    val name get() = this::class.simpleName!!
}

/**
 * A DataLoader is an object able to retrieve multiple values at once.
 * It needs to reference a `DataLoaderRef` representing its operation inside a DataLoaderExecutionScope
 *
 * It is associated to a Key and a Value, its implementation is going to receive a Set of Keys
 * representing the batch to be resolved.
 *
 * @param K the key to load the data
 * @param V the data to retrieve
 */
interface DataLoader<Ref: DataLoaderRef<K, V>, K : Any, V> {
    val ref: Ref

    /**
     * Load the set of `keys`, returning a Map mapping all the keys to their resolved value.
     */
    suspend fun load(keys: Set<K>): Map<K, V>

    /**
     * Used to modify the CoroutineContext used in the DataLoaderExecutionScope
     */
    fun register(ctx: CoroutineContext): CoroutineContext = ctx
}

/**
 * Constructor for batched data loader.
 * When multiple queries are run in the same execution the DataLoader will receive all of them as a Set of keys.
 * It can be used as:
 * ```
 * object MyOperation : DataLoaderRef<String, Int>
 *
 * val dataLoader = MyOperation.batched { keys ->
 *     keys.associateWith { it.length }
 * }
 * ```
 * @param block Main logic of the data loader, need to return a Map with an entry for every key in the input Set
 */
inline fun <Ref : DataLoaderRef<K, V>, K : Any, V> Ref.batched(crossinline block: suspend (Set<K>) -> Map<K, V>) =
    object : DataLoader<Ref, K, V> {
        override val ref = this@batched
        override suspend fun load(keys: Set<K>): Map<K, V> =
            block(keys)
    }

/**
 * Scope in which data loaders can be used.
 *
 * It makes DataLoaderRef executable, can be used through the DataLoaderRegistry as:
 * ```
 * object MyOperation : DataLoaderRef<String, Int>
 *
 * registry.withDataLoaders { // this: DataLoaderExecutionScope
 *     val deferredResult = MyOperation("something")
 * }
 * ```
 * When a DataLoaderRef is executed through it the batching and caching mechanism takes place
 */
interface DataLoaderExecutionScope {
    /**
     * Execute the query for the `key`
     * @param key the key to resolve
     * @return A Deferred that will resolve once the proper DataLoader will be executed during the next dispatch.
     */
    suspend operator fun  <K : Any, V> DataLoaderRef<K, V>.invoke(key: K): Deferred<V>

    suspend fun <V> DataLoaderEffect<V>.bind(): V = this.invoke(this@DataLoaderExecutionScope)
}

typealias DataLoaderEffect<V> = suspend DataLoaderExecutionScope.() -> V

fun <V> dataLoaderEffect(block: DataLoaderEffect<V>) = block
/**
 * Thrown when a data loader is invoked on a key, but it doesn't produce any value for it.
 */
class MissingResultException(val key: Any, val ref: DataLoaderRef<*, *>): IllegalStateException("Key [$key] is missing from results of data loader $ref")