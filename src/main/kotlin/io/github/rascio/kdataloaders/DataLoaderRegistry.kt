package io.github.rascio.kdataloaders

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import java.util.concurrent.Executors

class DataLoaderRegistry(private val eventListener: DataLoaderEventListener = DataLoaderEventListener {  }) {
    private val coroutineScope = CoroutineScope(
        context = CoroutineName("DataLoaderDispatcher")
            + SupervisorJob()
            + Executors.newSingleThreadExecutor()
                .asCoroutineDispatcher()
    )
    private val dataLoaders = mutableMapOf<DataLoaderRef<*, *>, DataLoader<*, *, *>>()
    operator fun plus(dataLoader: DataLoader<*, *, *>) =
        register(dataLoader).let { this }

    fun register(dataLoader: DataLoader<*, *, *>) {
        dataLoaders += (dataLoader.ref to dataLoader)
    }
    suspend fun <T> withDataLoaders(block: suspend DataLoaderExecutionScope.() -> T) = coroutineScope {
        // val coroutineScope = this
        CoroutineDataLoaderExecutionScope(coroutineScope, this@DataLoaderRegistry, eventListener)
            .block()
    }

    @Suppress("UNCHECKED_CAST")
    operator fun <Ref: DataLoaderRef<K, V>, K: Any, V> get(ref: Ref): DataLoader<Ref, K, V> =
        dataLoaders[ref]!! as DataLoader<Ref, K, V>

}
