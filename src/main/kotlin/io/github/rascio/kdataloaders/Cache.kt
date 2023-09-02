package io.github.rascio.kdataloaders

import kotlinx.coroutines.coroutineScope
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

fun <Ref : DataLoaderRef<K, V>, K : Any, V> DataLoader<Ref, K, V>.cached() = object : DataLoader<Ref, K, V> {
    override val ref = this@cached.ref

    val key = DataLoaderCache.DataLoaderCacheKey<K, V>()

    override suspend fun load(keys: Set<K>): Map<K, V> = coroutineScope {
        val cache = requireNotNull(coroutineContext[key]) { "Cache was not registered in the scope" }
        val cached = keys.mapNotNull { k ->
            cache[k]?.let { k to it}
        }.toMap()
        val missing = keys - cached.keys
        Logger.log("CachedDataLoader", "Load", "requested" to keys, "cached" to cached.keys, "missing" to missing)
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

class DataLoaderCache<K, V>(key: DataLoaderCacheKey<K, V>) : CoroutineContext.Element {
    class DataLoaderCacheKey<K, V> : CoroutineContext.Key<DataLoaderCache<K, V>>
    override val key: CoroutineContext.Key<*> = key

    private val cache = ConcurrentHashMap<K, V>()

    operator fun get(key: K) = cache[key]
    operator fun plusAssign(entries: Map<K, V>) {
        cache += entries
    }
}