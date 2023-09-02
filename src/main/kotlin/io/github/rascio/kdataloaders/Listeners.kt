package io.github.rascio.kdataloaders

import io.github.rascio.kdataloaders.CoroutineDataLoaderExecutionScope.Companion.DataLoaderEvent
import io.micrometer.core.instrument.Metrics
import java.util.concurrent.TimeUnit
import kotlin.reflect.full.memberProperties


object LoggerEventListener : DataLoaderEventListener, LogScope {
    override suspend fun invoke(event: DataLoaderEvent) =
        log(event.name, *event.toParams())

    private val DataLoaderEvent.name get() = this::class.simpleName!!

    fun DataLoaderEvent.toParams() =
        this::class.memberProperties
            .map { it.name to it.call(this) }
            .toTypedArray()
}

object MetricsEventListener : DataLoaderEventListener {

    private val appendCounter = Metrics.counter("kdataloader.append")
    private val dispatchRequestedCounter = Metrics.counter("kdataloader.dispatch.requested")
    private val dispatchStartedCounter = Metrics.counter("kdataloader.dispatch.started")


    override suspend fun invoke(event: DataLoaderEvent) = when (event){
        is DataLoaderEvent.AcquiringLock -> { }
        is DataLoaderEvent.CheckForBatching -> { }
        is DataLoaderEvent.DispatchAccepted -> { }
        is DataLoaderEvent.DispatchCompleted -> { }
        is DataLoaderEvent.DispatchRejected -> { }
        is DataLoaderEvent.DispatchRequested -> dispatchRequestedCounter.increment()
        is DataLoaderEvent.DispatchStarted -> dispatchStartedCounter.increment()
        is DataLoaderEvent.QueryAppended -> appendCounter.increment()
        is DataLoaderEvent.RefDispatchFailed -> { }
        is DataLoaderEvent.RefDispatchStarted -> { }
        is DataLoaderEvent.RefDispatchSucceed -> { }
        is DataLoaderEvent.WaitForBatching -> { }
    }
}