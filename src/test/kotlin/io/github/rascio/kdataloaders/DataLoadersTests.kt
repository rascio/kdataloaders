import arrow.atomic.AtomicBoolean
import arrow.atomic.AtomicInt
import arrow.fx.coroutines.CyclicBarrier
import io.github.rascio.kdataloaders.ChannelEventListener
import io.github.rascio.kdataloaders.CoroutineDataLoaderExecutionScope.Companion.DataLoaderEvent
import io.github.rascio.kdataloaders.DataLoaderEventLogger
import io.github.rascio.kdataloaders.DataLoaderRef
import io.github.rascio.kdataloaders.DataLoaderRegistry
import io.github.rascio.kdataloaders.LogScope
import io.github.rascio.kdataloaders.MissingResultException
import io.github.rascio.kdataloaders.batched
import io.github.rascio.kdataloaders.notify
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import java.util.TreeMap
import kotlin.test.assertEquals
import kotlin.test.assertTrue

typealias Person = String
object GetPersonById : DataLoaderRef<Int, Person>

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
class DataLoadersTests : LogScope {
    val DBeatles = TreeMap(
        mapOf(
            1 to "john",
            2 to "paul",
            3 to "ringo",
            4 to "george",
        )
    )

    private fun getPersonsById(keys: Set<Int>) = DBeatles.entries
        .filter { (id, _) -> id in keys }
        .associate { (id, p) -> id to p }

    @Test
    fun `should call a single data loader`() = runBlocking {
        val registry = DataLoaderRegistry(DataLoaderEventLogger) + GetPersonById.batched { keys ->
            getPersonsById(keys)
        }
        registry.withDataLoaders {
            val john = GetPersonById(1)

            log("Loading")

            withTimeout(100) {
                assertEquals("john", john.await())
            }
        }
    }

    @Test
    fun `should collapse same queries`() = runBlocking {
        val counter = AtomicInt()
        val registry = DataLoaderRegistry(DataLoaderEventLogger) + GetPersonById.batched { keys ->
            counter.incrementAndGet()
            // Given two queries are run concurrently
            // When requesting the same key
            // Then we will receive a single key for both of them
            assertEquals(1, keys.size)
            getPersonsById(keys)
        }
        registry.withDataLoaders {
            val john = GetPersonById(1)
            val anotherJohn = GetPersonById(1)

            log("Loading")

            withTimeout(100) {
                assertEquals("john", anotherJohn.await())
                assertEquals("john", john.await())
                //two queries with the same id will trigger the data loader only once
                assertEquals(1, counter.get())
            }
        }
    }

    @Test
    fun `should aggregate calls to same data loader`() = runBlocking {

        val registry = DataLoaderRegistry(DataLoaderEventLogger) + GetPersonById.batched { keys ->
            log("GetPersonById", "keys" to keys)
            // we will receive two keys if queries are batched
            assertEquals(2, keys.size)
            getPersonsById(keys)
        }
        registry.withDataLoaders {
            val john = GetPersonById(1)
            log("step1")
            val paul = GetPersonById(2)
            log("step2")

            withTimeout(500) {
                assertEquals("john", john.await())
                assertEquals("paul", paul.await())
            }
        }
    }

    @Test
    fun `should fail on missing key`(): Unit = runBlocking {
        val registry = DataLoaderRegistry(DataLoaderEventLogger) + GetPersonById.batched { keys ->
            getPersonsById(keys)
        }
        registry.withDataLoaders {
            val missing = GetPersonById(0)
            val john = GetPersonById(1)

            log("Loading")

            withTimeout(100) {
                assertEquals("john", john.await())
                val e = assertThrows<MissingResultException> {
                    missing.await()
                }
                log("ExceptionWasThrown", "message" to e.message)
                assertEquals(e.key, 0)
                assertEquals(e.ref, GetPersonById)
            }
        }
    }

    @Test
    fun `should wait to dispatch until other queries are published`(): Unit = runBlocking {
        (1..30).forEach {
            DBeatles += it to "Person$it"
        }
        // We will need to catch some diagnostic event
        val listener = ChannelEventListener { it is DataLoaderEvent.WaitForBatching }
        val registry = DataLoaderRegistry(DataLoaderEventLogger + listener) + GetPersonById.batched { keys ->
            // we expect 30 queries to be batched
            assertEquals(30, keys.size)
            getPersonsById(keys)
        }

        val allQueriesAreSent = AtomicBoolean(false)
        registry.withDataLoaders {
            val deferred = GetPersonById(1)
            val queries = mutableListOf<Deferred<Person>>()
            launch {
                while (queries.size < 30) {
                    // Wait for the next WaitForBatching event to be emitted
                    val (_, ack) = listener.out.receive()
                    log("SendNextQuery", "size" to queries.size)
                    queries += GetPersonById(queries.size + 1)
                    // tell the dispatcher it can resume
                    ack.notify()
                }
                log("AllQueriesSent", "size" to queries.size)
                allQueriesAreSent.set(true)
                val (_, ack) = listener.out.receive()
                // all queries has sent, the dispatcher will resume and dispatch all of previous queries
                ack.notify()
            }
            log("AccumulateQueriesJobStarted")
            withTimeout(1000) {
                val john = deferred.await()
                // all of them should have been dispatched already
                assertTrue(allQueriesAreSent.get(), "Not all queries are started")
                log("AllJobsAreRunningCheckPassed")

                assertEquals("Person1", john)
                queries.forEachIndexed { index, deferred ->
                    log("CheckJob", "id" to index, "person" to deferred.await())
                }
            }
        }
    }

    object GetPersonsByNameStartingWith : DataLoaderRef<String, List<Person>>
    @Test
    fun `should parallelize calls to different data loaders`() = runBlocking {

        val barrier = CyclicBarrier(2)

        val registry = DataLoaderRegistry(DataLoaderEventLogger) + GetPersonById.batched { keys ->
            assertEquals(2, keys.size)
            barrier.await()
            getPersonsById(keys)
        } + GetPersonsByNameStartingWith.batched { keys ->
            barrier.await()
            keys.associateWith { k ->
                DBeatles.entries
                    .filter { it.value.startsWith(k) }
                    .map { it.value }
                    .toList()
            }
        }
        registry.withDataLoaders {
            val john = GetPersonById(1)
            log("step1")
            val paul = GetPersonById(2)
            log("step2")
            val ringo = GetPersonsByNameStartingWith("ri")
            log("step3")

            withTimeout(500) {
                assertEquals("john", john.await())
                assertEquals("paul", paul.await())
                assertEquals(listOf("ringo"), ringo.await())
            }
        }
    }

    @Test
    fun `should parallelize and batch calls to different data loaders on multiple threads`(): Unit = runBlocking {
        withContext(Dispatchers.IO) {

            val barrier = CyclicBarrier(2)
            val listener = ChannelEventListener {
                (it is DataLoaderEvent.DispatchRequested && it.query.key in listOf(1, 2))
            }

            val registry = DataLoaderRegistry(DataLoaderEventLogger + listener) + GetPersonById.batched { keys ->
                barrier.await()
                assertEquals(2, keys.size)
                getPersonsById(keys)
            } + GetPersonsByNameStartingWith.batched { keys ->
                barrier.await()
                keys.associateWith { k ->
                    DBeatles.entries
                        .filter { it.value.startsWith(k) }
                        .map { it.value }
                        .toList()
                }
            }
            registry.withDataLoaders {
                val john = async { GetPersonById(1).await() }
                launch {
                    val paul = GetPersonById(2)
                    yield()
                    val ringo = GetPersonsByNameStartingWith("ri")

                    withTimeout(500) {
                        assertEquals("john", john.await())
                        assertEquals("paul", paul.await())
                        assertEquals(listOf("ringo"), ringo.await())
                    }
                }
                // Wait for both john and paul to arrive to the DispatchRequest
                // so that we are sure the two threads are going in parallel
                val (_, ackFirst) = listener.out.receive()
                val (_, ackSecond) = listener.out.receive()
                ackSecond.notify()
                ackFirst.notify()

            }
        }
    }

    @Test
    fun `should not batch if another data loader miss the dispatch`() = runBlocking {
        withContext(Dispatchers.IO) {
            val getPersonIdExecutions = AtomicInt()
            val listener = ChannelEventListener {
                (it is DataLoaderEvent.DispatchCompleted && it.query.key == 1)
                        || (it is DataLoaderEvent.AcquiringLock && it.query.key == 2)
            }
            val registry = DataLoaderRegistry(DataLoaderEventLogger + listener) + GetPersonById.batched { keys ->
                log("GetPersonById", "keys" to keys)
                getPersonIdExecutions.incrementAndGet()
                assertEquals(1, keys.size) //expecting 1 means no batching
                getPersonsById(keys)
            }
            registry.withDataLoaders {
                val john = GetPersonById(1)
                log("step1")

                withTimeout(1000) {
                    val (_, ack) = listener.out.receive()
                    // after receiving the DispatchCompleted run another data loader
                    val paul = async {
                        GetPersonById(2).await()
                    }
                    // wait for this second one to try to AcquireLock
                    // to sync the coroutines
                    val (_, ackSecondQuery) = listener.out.receive()
                    ackSecondQuery.notify()
                    // wait for paul to try to append in the dispatching
                    delay(10)
                    ack.notify()
                    assertEquals("john", john.await())
                    //get person id should have been executed only once when "john" returns from the await
                    assertEquals(1, getPersonIdExecutions.get())
                    assertEquals("paul", paul.await())
                    assertEquals(2, getPersonIdExecutions.get())
                }
            }
        }
    }

    object SlowOperation : DataLoaderRef<String, String>
    @Test
    fun `slow operations should not block faster operations`() = runBlocking {
        val completeSlowOperation = Channel<Unit>()
        val registry = DataLoaderRegistry(DataLoaderEventLogger) +
                GetPersonById.batched { keys ->
                    getPersonsById(keys)
                } +
                SlowOperation.batched { keys ->
                    completeSlowOperation.receive()
                    keys.associateWith { "result" }
                }

        registry.withDataLoaders {
            val john = GetPersonById(1)
            val slowOperation = SlowOperation("op")
            withTimeout(1000) {
                assertEquals("john", john.await())
                // slowOperation will be completed after john, because of the 'notify()'
                // this prove that they are resolved independently, and the CompletableDeferred
                // of the fast one is completed before the slow one
                completeSlowOperation.notify()
                assertEquals("result", slowOperation.await())
            }
        }
    }
}
