package io.github.rascio.kdataloaders

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withContext
import java.util.concurrent.Executors
import kotlin.coroutines.CoroutineContext

/**
 * Registry of data loaders.
 * It is used to register the implementation that is triggered by a `DataLoaderRef`.
 *
 * There are two ways to register DataLoaders in the registry:
 * ```
 * val dataLoader1: DataLoader<String, Int>
 * val dataLoader2: DataLoader<String, Long>
 *
 * // using register method
 * val registry = DataLoaderRegistry()
 * registry.register(dataLoader1)
 * registry.register(dataLoader2)
 *
 * //using operators
 * val registry = DataLoaderRegistry() + dataLoader1 + dataLoader2
 * ```
 *
 * It can then be used to retrieve the data loader scope and execute queries with it, as:
 * ```
 * object MyQuery : DataLoaderRef<String, Int>
 *
 * fun main() {
 *    val registry = DataLoaderRegistry() + MyQuery.batched { keys ->
 *       keys.associateWith { it.size }
 *    }
 *    runBlocking {
 *        registry.withDataLoaders {
 *            val v1 = MyQuery("one")
 *            val v2 = MyQuery("two")
 *            val v3 = MyQuery("three")
 *
 *            check(v1.await() == 3)
 *            check(v2.await() == 3)
 *            check(v3.await() == 5)
 *        }
 *    }
 * }
 * ```
 */
class DataLoaderRegistry(private val eventListener: DataLoaderEventListener.Factory = NoOpListener) {
    /*
    private val coroutineScope = CoroutineScope(
        context = CoroutineName("DataLoaderDispatcher")
            + SupervisorJob()
            + Executors.newSingleThreadExecutor()
                .asCoroutineDispatcher()
    )
     */
    private val dataLoaders = mutableMapOf<DataLoaderRef<*, *>, DataLoader<*, *, *>>()
    operator fun plus(dataLoader: DataLoader<*, *, *>) =
        register(dataLoader).let { this }

    fun register(dataLoader: DataLoader<*, *, *>) {
        dataLoaders += (dataLoader.ref to dataLoader)
    }
    suspend fun <T> withDataLoaders(block: suspend DataLoaderExecutionScope.() -> T) = coroutineScope {
        var coroutineCtx = coroutineContext
        for (dataLoader in dataLoaders.values) {
            coroutineCtx = dataLoader.register(coroutineCtx)
        }
        withContext(coroutineCtx) {
            CoroutineDataLoaderExecutionScope(
                dataLoaderCoroutineScope = this,
                registry = this@DataLoaderRegistry,
                eventListener = eventListener.create()
            ).block()
        }
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <Ref: DataLoaderRef<K, V>, K: Any, V> get(ref: Ref): DataLoader<Ref, K, V> =
        checkNotNull(dataLoaders[ref]) { "DataLoader for $ref was invoked, but not registered" }
                as DataLoader<Ref, K, V>

}
