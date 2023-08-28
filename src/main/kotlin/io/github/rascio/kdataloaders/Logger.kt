package io.github.rascio.kdataloaders
import kotlin.reflect.jvm.jvmName

object Logger : LogScope {
    val start = System.currentTimeMillis()
    // Everything after this is in red
    private val time = "\u001b[91m"
    private val thread = "\u001b[1;31m"
    private val tag = "\u001b[32m"
    private val key = "\u001b[34m"
    private val value = "\u001b[37m"

    // Resets previous color codes
    private val reset = "\u001b[0m"
    fun log(label: String, msg: String) {
        println("$thread${System.currentTimeMillis() - start}ms$reset | $time${Thread.currentThread().name}$reset | $tag$label$reset | $msg")
    }
    fun log(label: String, msg: String, vararg data: Pair<String, Any?>) {
        log(label, "$msg${data.asStr()}")
    }
    fun log(label: String, msg: String, t: Throwable) {
        log(label, msg)
        t.printStackTrace()
    }
    fun log(label: String, msg: String, t: Throwable, vararg data: Pair<String, Any>) {
        log(label, msg, *data)
        t.printStackTrace()
    }
    private fun Array<out Pair<String, Any?>>.asStr() =
        joinToString(prefix = "\n\t", separator = "\n\t") { (k, v) -> "$key$k$reset = $value$v$reset" }
}
interface LogScope {
    fun log(msg: String, vararg data: Pair<String, Any?>) {
        Logger.log(label = logTag, msg = msg, data = data)
    }
    fun log(msg: String) {
        Logger.log(logTag, msg)
    }
    fun log(msg: String, t: Throwable, vararg data: Pair<String, Any>) {
        Logger.log(logTag, msg, t, data = data)
    }

    val logTag: String get() =
        if (this::class.isCompanion)
            this::class.java.declaringClass.simpleName
        else
            this::class.simpleName ?: this::class.jvmName
}
