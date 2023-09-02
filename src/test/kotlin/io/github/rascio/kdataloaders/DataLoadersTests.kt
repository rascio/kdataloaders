import arrow.atomic.AtomicBoolean
import arrow.atomic.AtomicInt
import arrow.fx.coroutines.CyclicBarrier
import io.github.rascio.kdataloaders.ChannelEventListener
import io.github.rascio.kdataloaders.CoroutineDataLoaderExecutionScope.Companion.DataLoaderEvent
import io.github.rascio.kdataloaders.LoggerEventListener
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
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.util.TreeMap
import kotlin.random.Random
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
        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
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
        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
            counter.incrementAndGet()
            // Given two queries are run concurrently
            // When requesting the same key
            // Then we will receive a single key for both of them
            check(1 == keys.size)
            getPersonsById(keys)
        }
        registry.withDataLoaders {
            val paul = GetPersonById(2)
            val anotherPaul = GetPersonById(2)

            log("Loading")

            withTimeout(100) {
                assertEquals("paul", anotherPaul.await())
                assertEquals("paul", paul.await())

                //two queries with the same id will trigger the data loader only once
                assertEquals(1, counter.get())
            }
        }
    }

    @Test
    fun `should aggregate calls to same data loader`() = runBlocking {

        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
            log("GetPersonById", "keys" to keys)
            // we will receive two keys if queries are batched
            check(2 == keys.size)
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
        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
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
    fun `should aggregate calls to same data loader on different coroutines`(): Unit = runBlocking {

        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
            log("GetPersonById", "keys" to keys)
            // we will receive two keys if queries are batched
            check(2 == keys.size)
            getPersonsById(keys)
        }
        registry.withDataLoaders {
            listOf(
                launch {
                    val john = GetPersonById(1)
                    log("step1")
                    withTimeout(500) {
                        assertEquals("john", john.await())
                    }
                },
                launch {
                    val paul = GetPersonById(2)
                    log("step2")
                    withTimeout(500) {
                        assertEquals("paul", paul.await())
                    }
                }
            ).joinAll()
        }
    }
    @ParameterizedTest
    @ValueSource(ints = [2, 5, 10, 30, 60, 100])
    fun `should wait to dispatch until other queries are published`(concurrency: Int): Unit = runBlocking {

        (1..concurrency).forEach {
            DBeatles += it to "Person$it"
        }
        // We will need to catch some diagnostic event
        //val listener = ChannelEventListener { it is DataLoaderEvent.WaitForBatching }
        val registry = DataLoaderRegistry(LoggerEventListener /*+ listener*/) + GetPersonById.batched { keys ->
            // we expect 30 queries to be batched
            check(concurrency == keys.size) { "$concurrency != ${keys.size}" }
            getPersonsById(keys)
        }

        val allQueriesAreSent = AtomicBoolean(false)
        registry.withDataLoaders {
            val deferred = GetPersonById(1)
            val queries = mutableListOf<Deferred<Person>>()
            launch {
                while (queries.size < concurrency) {
                    // Wait for the next WaitForBatching event to be emitted
                    //val (_, ack) = listener.out.receive()
                    log("SendNextQuery", "size" to queries.size)
                    queries += GetPersonById(queries.size + 1)
                    // tell the dispatcher it can resume
                    //ack.notify()
                }
                log("AllQueriesSent", "size" to queries.size)
                allQueriesAreSent.set(true)
                //val (_, ack) = listener.out.receive()
                // all queries has sent, the dispatcher will resume and dispatch all of previous queries
                //ack.notify()
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

        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
            check(2 == keys.size)
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
    fun `should parallelize and batch calls to different data loaders on multiple threads`(): Unit = runBlocking(Dispatchers.IO) {

        val barrier = CyclicBarrier(2)

        val registry = DataLoaderRegistry(LoggerEventListener) + GetPersonById.batched { keys ->
            barrier.await()
            check(2 == keys.size)
            getPersonsById(keys)
        } + GetPersonsByNameStartingWith.batched { keys ->
            barrier.await()
            keys.associateWith { k ->
                DBeatles.values
                    .filter { it.startsWith(k) }
                    .toList()
            }
        }
        registry.withDataLoaders {
            listOf(
                launch {
                    val john = GetPersonById(1)
                    withTimeout(500) {
                        assertEquals("john", john.await())
                    }
                },
                launch {
                    val paul = GetPersonById(2)

                    withTimeout(500) {
                        assertEquals("paul", paul.await())
                    }
                },
                launch {
                    val ringo = GetPersonsByNameStartingWith("ri")

                    withTimeout(500) {
                        assertEquals(listOf("ringo"), ringo.await())
                    }
                }
            ).joinAll()
        }
    }

    object SlowOperation : DataLoaderRef<String, String>
    @Test
    fun `slow operations should not block faster operations`() = runBlocking {
        val completeSlowOperation = Channel<Unit>()
        val registry = DataLoaderRegistry(LoggerEventListener) +
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

    object LoadSomething : DataLoaderRef<String, String>
    @ParameterizedTest
    @ValueSource(ints = [2, 10, 50, 100, 1000])
    fun `should aggregate queries`(concurrency: Int): Unit = runBlocking {
        //withContext(Dispatchers.IO) {
            val registry = DataLoaderRegistry(LoggerEventListener) + LoadSomething.batched { keys ->
                check(keys.size == concurrency) { "Should aggregate $concurrency keys, but they were ${keys.size}" }
                keys.associateWith { it.lowercase() }
            }

            registry.withDataLoaders {
                (1..concurrency).map {
                    launch {
                        var s = "$it-"
                        repeat(Random.nextInt(20, 30)) {
                            delay(Random.nextLong(0, 3))
                            s = "$s${Random.nextInt('A'.code, 'Z'.code).toChar()}"
                        }
                        val query = LoadSomething(s)
                        withTimeout(1000) {
                            assertEquals(s.lowercase(), query.await())
                        }
                    }
                }.joinAll()
            }
        //}
    }
}
