package io.github.rascio.kdataloaders

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob

class DataLoaderRegistry(private val eventListener: DataLoaderEventListener = DataLoaderEventListener {  }) {
    private val coroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val dataLoaders = mutableMapOf<DataLoaderRef<*, *>, DataLoader<*, *, *>>()
    operator fun plus(dataLoader: DataLoader<*, *, *>) =
        register(dataLoader).let { this }

    fun register(dataLoader: DataLoader<*, *, *>) {
        dataLoaders += (dataLoader.ref to dataLoader)
    }
    suspend fun <T> withDataLoaders(block: suspend DataLoaderExecutionScope.() -> T) =
        CoroutineDataLoaderExecutionScope(coroutineScope, this@DataLoaderRegistry, eventListener)
            .block()

    operator fun <Ref: DataLoaderRef<K, V>, K: Any, V> get(ref: Ref): DataLoader<Ref, K, V> =
        dataLoaders[ref]!! as DataLoader<Ref, K, V>

    companion object {
//        fun DataLoaderRegistry.dataLoaderScope() =
//            CoroutineDataLoaderExecutionScope(this, this.eventListener)
    }
}
