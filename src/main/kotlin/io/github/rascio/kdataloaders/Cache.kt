package io.github.rascio.kdataloaders

import kotlinx.coroutines.coroutineScope
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

/**
 * Enable caching capability to a DataLoader.
 * When caching is enabled when a key is loaded will be cached in the current DataLoaderExecutionScope,
 * and its DataLoader will not be called anymore when requesting the same cache key.
 *
 * If during the execution multiple keys are requested, the data loader will be invoked using only the keys missing in the cache.
 */
fun <Ref : DataLoaderRef<K, V>, K : Any, V> DataLoader<Ref, K, V>.cached() = object : DataLoader<Ref, K, V> {
    override val ref = this@cached.ref

    val key = DataLoaderCache.DataLoaderCacheKey<K, V>()

    override suspend fun load(keys: Set<K>): Map<K, V> = coroutineScope {
        val cache = requireNotNull(coroutineContext[key]) { "Cache was not registered in the scope" }
        val cached = keys.mapNotNull { k ->
            cache[k]?.let { k to it}
        }.toMap()
        val missing = keys - cached.keys
        if (missing.isEmpty()) {
            cached
        } else {
            val results = this@cached.load(missing)
            cache += results
            results + cached
        }
    }

    override fun register(ctx: CoroutineContext): CoroutineContext =
        ctx + DataLoaderCache(key)
}

/**
 * The cache takes advantage of the coroutine context to store the values.
 */
internal class DataLoaderCache<K, V>(key: DataLoaderCacheKey<K, V>) : CoroutineContext.Element {
    class DataLoaderCacheKey<K, V> : CoroutineContext.Key<DataLoaderCache<K, V>>
    override val key: CoroutineContext.Key<*> = key

    private val cache = ConcurrentHashMap<K, V>()

    operator fun get(key: K) = cache[key]
    operator fun plusAssign(entries: Map<K, V>) {
        cache += entries
    }
}