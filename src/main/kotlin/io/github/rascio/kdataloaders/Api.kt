package io.github.rascio.kdataloaders

import kotlinx.coroutines.Deferred

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
interface DataLoaderRef<K : Any, V>

/**
 * A DataLoader is an object able to retrieve multiple values at once.
 * @param K the key to load the data
 * @param V the data to retrieve
 */
interface DataLoader<Ref: DataLoaderRef<K, V>, K : Any, V> {
    val ref: Ref
    suspend fun load(keys: Set<K>): Map<K, V>
}

/**
 * Constructor for batched data loader.
 * When multiple queries are run in the same execution the DataLoader will receive all of them as a Set of keys.
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
 */
interface DataLoaderExecutionScope {
    suspend operator fun  <K : Any, V> DataLoaderRef<K, V>.invoke(key: K): Deferred<V>
}

class MissingResultException(val key: Any, val ref: DataLoaderRef<*, *>): IllegalStateException("Key [$key] is missing from results of data loader $ref")