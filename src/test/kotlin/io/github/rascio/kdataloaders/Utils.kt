package io.github.rascio.kdataloaders

import io.github.rascio.kdataloaders.CoroutineDataLoaderExecutionScope.Companion.DataLoaderEvent
import kotlinx.coroutines.channels.Channel
import kotlin.reflect.full.memberProperties


class ChannelEventListener(private val predicate: (DataLoaderEvent) -> Boolean) : DataLoaderEventListener, LogScope {
    val out = Channel<Pair<DataLoaderEvent, Channel<Unit>>>()
    override suspend fun invoke(event: DataLoaderEvent) {
        val ack = Channel<Unit>()
        if (predicate(event)) {
            out.send(event to ack)
            log("WaitNotification", "event" to event)
            ack.receive()
        }
    }
}

suspend fun Channel<Unit>.notify() =
    send(Unit)