# kdataloaders

Data loaders implementation using coroutines.

# Getting Started

The library is meant to be used to declare Data Loaders objects and use them inside specific Kotlin scope, like:

```kotlin
executeWithDataLoaderScope {
    launch {
        val personA = findPersonById(1)
        println("personA = $personA")
    }
    launch {
        val personB = findPersonById(2)
        println("personB = $personB")
    }
}
```
when using data loaders inside its scope (`findByPerson(Int)`), will batch together multiple calls, also dispatched by different threads.

Maybe one day I will provide better docs, for now start [having a look at the APIs](https://github.com/rascio/kdataloaders/blob/main/src/main/kotlin/io/github/rascio/kdataloaders/Api.kt)

Some example usages are available in [the main test class](https://github.com/rascio/kdataloaders/blob/main/src/test/kotlin/io/github/rascio/kdataloaders/DataLoadersTests.kt)

All the trickery and magic is done inside [the Data Loaders scope implementation](https://github.com/rascio/kdataloaders/blob/main/src/main/kotlin/io/github/rascio/kdataloaders/CoroutineDataLoaderExecutionScope.kt)
