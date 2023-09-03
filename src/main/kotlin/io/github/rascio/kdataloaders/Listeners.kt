package io.github.rascio.kdataloaders

import io.github.rascio.kdataloaders.CoroutineDataLoaderExecutionScope.Companion.DataLoaderEvent
import io.micrometer.core.instrument.Metrics
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.full.memberProperties

private val DataLoaderEvent.name get() = this::class.simpleName!!

object NoOpListener : DataLoaderEventListener, DataLoaderEventListener.Factory {
    override suspend fun invoke(event: DataLoaderEvent) { }
    override fun create() = this
}

object LoggerEventListener : DataLoaderEventListener, DataLoaderEventListener.Factory, LogScope {
    override fun create() = this

    override suspend fun invoke(event: DataLoaderEvent) =
        log(event.name, *event.toParams())

    fun DataLoaderEvent.toParams() =
        this::class.memberProperties
            .map { it.name to it.call(this) }
            .toTypedArray()
}

object MetricsEventListener : DataLoaderEventListener.Factory {

    override fun create() = object : DataLoaderEventListener {

        private val timers = ConcurrentHashMap<String, Long>()

        override suspend fun invoke(event: DataLoaderEvent) {
            when (event) {
                is DataLoaderEvent.AcquiringLock -> increment(event, event.query.ref)
                is DataLoaderEvent.CheckForBatching -> { }
                is DataLoaderEvent.DispatchAccepted -> increment(event, event.query.ref)
                is DataLoaderEvent.DispatchCompleted -> increment(event, event.query.ref)
                is DataLoaderEvent.DispatchRejected -> increment(event, event.query.ref)
                is DataLoaderEvent.DispatchRequested -> increment(event, event.query.ref)
                is DataLoaderEvent.DispatchStarted -> {
                    increment(event, event.query.ref)
                    timeFromLast(event)
                }
                is DataLoaderEvent.QueryAppended -> increment(event, event.query.ref)
                is DataLoaderEvent.RefDispatchFailed -> increment(event, event.ref)
                is DataLoaderEvent.RefDispatchStarted -> increment(event, event.ref)
                is DataLoaderEvent.RefDispatchSucceed -> {
                    increment(event, event.ref)
                    addValue(event, event.ref, event.keys.size)
                }
                is DataLoaderEvent.WaitForBatching -> { }
            }
        }

        private fun timeFromLast(event: DataLoaderEvent) {
            val name = metric(event)
            timers.put(name, event.time)?.also { previous ->
                Metrics.timer(name).record(Duration.ofMillis(event.time - previous))
            }
        }
    }

    private fun increment(event: DataLoaderEvent, type: DataLoaderRef<*, *>) =
        Metrics.counter(metric(event), type.name).increment()

    private fun addValue(event: DataLoaderEvent, type: DataLoaderRef<*, *>, v: Number) =
        Metrics.summary(metric(event), type.name).record(v.toDouble())


    private fun metric(event: DataLoaderEvent) =
        "kdataloader.events.${event.name}"
}