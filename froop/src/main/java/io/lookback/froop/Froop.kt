@file:Suppress("NestedLambdaShadowedImplicitParameter", "MemberVisibilityCanBePrivate")

package io.lookback.froop

import java.lang.ref.WeakReference
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock

//
//  port of froop.swift
//  Created by bhicks on 2019-05-03.
//  Copyright © 2019 Lookback Ltd. All rights reserved.
//
//  Original froop.swift Created by martin on 2019-02-23.
//
//  Global singelton that can redirect the froop logging from .debug()
var froopLog: (String, String) -> Unit = { label, message ->
    print("$label $message\n")
}

// Stream of values over time. Typically created by a FSink.
// ```
// let sink = FSink<Int>()
//
// let stream = sink.stream()
// let double = stream.map() { $0 * 2 }
//
// sink.update(0)
// sink.update(1)
// ...
// ```
//
// For some detailed notes on how a combinator is structured for ARC and thread
// safety, see the source code for the `.map()` function. All operations follow
//  a similar pattern.


open class FStream<T> {

    companion object {

        private var streamCount: Locker<Long> = Locker(value = 0)

        // Create a stream that never emits anything. It stays inerts forever.

        fun <T> never(): FStream<T> {
            val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
            stream.inner.withValue { it.update(null) }
            return stream
        }

        @Suppress("unused")
        fun <T> of(t: T): FMemoryStream<T> {
            val stream = FMemoryStream<T>(memoryMode = MemoryMode.AfterEnd)
            stream.inner.withValue { it.update(t) }
            return stream
        }
    }

    val inner: Locker<Inner<T>>
    var parent: Peg? = null

    internal val ident: Long = streamCount.withAndSetValue {
        it + 1
    }

    override operator fun equals(other: Any?): Boolean {
        if (other == null) {
            return false
        }

        if (other is FStream<*>) {
            val otherStream = other as? FStream<*>
            return ident == otherStream?.ident
        }
        return false
    }

    /** Check if this stream is in memory mode. */
    val isMemory: Boolean
        get() = this.inner.withValue { it.memoryMode.isMemory() }

    // a new stream with a new inner
    constructor(memoryMode: MemoryMode) {
        inner = Locker(value = Inner(memoryMode))
    }

    // a new stream with a cloned inner
    constructor(inner: Locker<Inner<T>>) {
        this.inner = inner
    }

    // Subscribe to values from this stream.
    fun subscribe(listener: (T) -> Unit): Subscription =
        inner.withValue {
            val strong = it.subscribeStrong(peg = this.parent) {
                if (it != null) {
                    listener(it)
                }
            }
            Subscription(strong)
        }

    // Subscribe to the end of the stream
    fun subscribeEnd(listener: () -> Unit): Subscription =
        inner.withValue {
            val strong = it.subscribeStrong(peg = this.parent) {
                if (it == null) {
                    listener()
                }
            }
            Subscription(strong)
        }

    // Internal subscribe that returns a `Peg` which is used to keep
    // a weak reference alive of a listener to the parent stream.
    fun subscribeInner(listener: (T?) -> Unit): Peg {
        // Peg for the new weak subscription.
        val peg = this.inner.withValue { it.subscribeWeak(onvalue = listener) }
        // This peg must also keep the parent stream alive.
        // This is for chained operators such as .map().filter().map()
        // where the intermediaries would be unsubscribed otherwise.
        peg.parent = this.parent
        return peg
    }

    // Collect values of this stream into an array of values.
    fun collect(): Collector<T> {
        val c = Collector<T>()
        c.parent = subscribeInner(listener = c::update)
        return c
    }


    // Print every object passing through this stream prefixed by the `label`.
    fun debug(label: String): FStream<T> =
        map {
            froopLog(label, it.toString())
            it
        }

    // Dedupe the stream by extracting some equatable value from it.
    // The value is compared for consecutive elements.
    fun <U> dedupeBy(f: (T) -> U): FStream<T> {
        return dedupeBy(f = f, memory = false)
    }

    open fun <U> dedupeBy(f: (T) -> U, memory: Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        var lastU: U? = null
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                val newU = f(t)
                if (lastU != newU) {
                    lastU = newU
                    inner.withValue { it.update(t) }
                }
            } else {
                inner.withValue { it.update(null) }
            }
        }
        return stream
    }

    // Dedupe the stream by the value in the stream itself
    fun dedupe(): FStream<T> = this.dedupeBy { it }

    fun dedupe(memory: Boolean): FStream<T> = this.dedupeBy({ it }, memory)

    // Drop a fixed number of initial values, then start emitting.
    fun drop(amount: Long): FStream<T> {
        return drop(amount = amount, memory = false)
    }

    fun drop(amount: Long, memory: Boolean): FStream<T> {
        var todo = amount + 1
        return dropWhile({
            if (todo > 0) {
                todo -= 1
            }
            todo > 0
        }, memory = memory)
    }

    // Drop values while some condition holds true, then start emitting.
    // Once started emitting, it will never drop again.
    fun dropWhile(f: (T) -> Boolean): FStream<T> {
        return dropWhile(f = f, memory = false)
    }

    fun dropWhile(f: (T) -> Boolean, memory: Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        var dropping = true
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                if (dropping) {
                    dropping = f(t)
                }
                if (!dropping) {
                    inner.withValue { it.update(t) }
                }
            } else {
                inner.withValue { it.update(null) }
            }
        }
        return stream
    }

    // Make a stream that ends when some other stream ends.
    fun <U> endWhen(other: FStream<U>): FStream<T> {
        return endWhen(other = other, memory = false)
    }

    fun <U> endWhen(other: FStream<U>, memory: Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        val p1 = subscribeInner { t ->
            // regular values or end, both are propagated
            inner.withValue { it.update(t) }
        }
        val p2 = other.subscribeInner {
            // ignore regular values, just look out for the end
            if (it == null) {
                inner.withValue { it.update(null) }
            }
        }
        // peg both parent and other
        stream.parent = Peg(pegs = mutableListOf(p1, p2))
        return stream
    }

    // Filter the stream using some sort of test.
    fun filter(f: (T) -> Boolean): FStream<T> {
        return filter(f = f, memory = false)
    }

    fun filter(f: (T) -> Boolean, memory: Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                if (f(t)) {
                    inner.withValue {
                        it.update(t)
                    }
                }
            } else {
                inner.withValue {
                    it.update(null)
                }
            }
        }
        return stream
    }

    // Filter the stream and also transform the value.
    fun <U> filterMap(f: (T) -> U?): FStream<U> {
        return filterMap(f = f, memory = false)
    }

    fun <U> filterMap(f: (T) -> U?, memory: Boolean): FStream<U> {
        val stream = FStream<U>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                val u = f(t)
                if (u != null) {
                    inner.withValue {
                        it.update(u)
                    }
                }
            } else {
                inner.withValue {
                    it.update(null)
                }
            }
        }
        return stream
    }

    /**
     * Filter this stream to a subtype.
     */
    @Suppress("UNCHECKED_CAST")
    fun <R> narrow(test: (T) -> Boolean): FStream<R> =
        this.filter(test) as FStream<R>

    /** Narrow to the type parameter. */
    inline fun <reified R> narrow(): FStream<R> = narrow { it is R }

    // Fold the stream by combining values from the past with the new value.
    // The seed is emitted as the first value.
    //
    // This is roughly equivalent of an `Array.reduce()` or `Array.fold()`.
    fun <U> fold(seed: U, f: (U, T) -> U): FMemoryStream<U> {
        val stream = FMemoryStream<U>(memoryMode = MemoryMode.UntilEnd)
        val inner = stream.inner
        // emit seed as first value
        inner.withValue { it.update(seed) }
        // keep track of previous value
        var prev = seed
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                val next = f(prev, t)
                prev = next
                inner.withValue {
                    it.update(next)
                }
            } else {
                inner.withValue {
                    it.update(null)
                }
            }
        }
        return stream
    }

    // Internal function that starts an imitator.
    fun attachImitator(imitator: FImitator<T>): Subscription {
        val imitInner = imitator.inner
        val parentPeg = this.parent
        return this.inner.withValue {

            val lock = ReentrantLock(true) // a fair lock should guarantee order
            var imitationQueue: MutableList<T?> = mutableListOf()

            fun takeTodo(): MutableList<T?> {
                val t = imitationQueue // get all from list
                imitationQueue = mutableListOf() // clear list
                return t // return those that were got
            }

            val strong = it.subscribeStrong(peg = parentPeg) { t ->
                // the observed order of values of this subscribe must be preserved
                // we put each value into the imitationQueue and ensure the array
                // is processed in order.
                lock.lock()
                imitationQueue.add(t)
                lock.unlock()

                // an imitation is a closure that captures the value to be
                // dispatched later into the imitator. the closure is added to a
                // thread local and is called later, after the current evaluation
                // finishes.
                val newTodo: Imitation = {
                    imitInner.withValue { imit ->
                        val toDispatch = {
                            lock.lock()
                            val x = takeTodo()
                            lock.unlock()
                            x
                        }()
                        toDispatch.forEach { imit.update(it) }
                    }
                }
                // add to thread local to be executed after current tree eval
                imitations.withValue {
                    // it is a ThreadLocal whose value is a list of Imitations
                    // add this to the imitationQueue
                    val v: ImitationRunner? = it.get()
                    v?.append(newTodo)
                }
            }
            Subscription(strong)
        }
    }

    // Makes a stream that only emits the last value.
    fun last(): FStream<T> {
        return last(memory = false)
    }

    fun last(memory: Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        var lastValue: T? = null
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                lastValue = t
            } else {
                inner.withValue {
                    if (lastValue != null) {
                        it.update(lastValue)
                    }
                    it.update(null)
                }
            }
        }
        return stream
    }

    // Transform values of type T to type U.
    fun <U> map(f: (T) -> U): FStream<U> {
        return map(f = f, memory = false)
    }

    fun <U> map(f: (T) -> U, memory: Boolean): FStream<U> {
        // We want to add a listener to `self` and create a new `FStream` instance
        // that receives updates from that listener and apply the transform
        // `f` to incoming values.
        //
        // However if we drop the new stream instance, we don't want it to be
        // kept alive by a strong reference from the listener.
        //
        // Consider a stream of streams:
        // ```
        //   let s: FStream<FStream<Int>> = ...
        //   let x: FStream<Int> = s.map() { innerStream ->
        //      innerStream.map() { $0 + 1 }
        //   }
        //   .flatten()
        // ```
        //
        // The inner `.map()` instance will be dropped/recreated for every innerStream
        // value. It's clear we don't want it to be kept alive after the .filter() instance
        // drops.
        //
        // This is where `subscribeInner` comes in. It adds a weak listener to `self`
        // and returns a `Peg` that is an opaque wrapper for the strong reference to
        // the listener. That "peg" then lives inside the new stream instance and thus
        // when they drop together, we automatically "unsubscribe" the weak listener.
        val stream = FStream<U>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        // We can't use "stream" inside the closure since that would capture the stream instance
        // and thus also the "peg" described above (we would get a cyclic dependency keeping the
        // tree alive). So we get an ARC reference to the inner, which is used to dispatch values.
        val inner = stream.inner
        // We subscribe to self and back comes the "peg" that goes into the new stream.
        stream.parent = subscribeInner {
            // If it is a value, transform it otherwise end.
            val t = it
            if (t != null) {
                inner.withValue { it.update(f(t)) }
            } else {
                inner.withValue { it.update(null) }
            }
        }
        return stream
    }

    // Transform any incoming value to one fixed value.
    fun <U> mapTo(value: U): FStream<U> {
        return mapTo(value = value, memory = false)
    }

    fun <U> mapTo(value: U, memory: Boolean): FStream<U> {
        val stream = FStream<U>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        stream.parent = subscribeInner {
            if (it != null) {
                inner.withValue { it.update(value) }
            } else {
                inner.withValue { it.update(null) }
            }
        }
        return stream
    }

    // Make a stream that remembers the last value. Any new (or combinator) will get the
    // last emitted value straight away.
    fun remember(): FMemoryStream<T> {
        val stream = FMemoryStream<T>(memoryMode = MemoryMode.UntilEnd)
        val inner = stream.inner
        stream.parent = subscribeInner { t ->
            inner.withValue {
                it.update(t)
            }
        }
        return stream
    }

    // For every value of this stream, take a sample of the last value of some other
    // stream and combine the two.
    //
    // This stream becomes a kind of "trigger" for a value that is a combination of two.
    // Useful when wanting to filter/gate one stream on a value from some other stream.
    //
    // No value will be emitted unless `other` has produced at least one value.
    fun <U> sampleCombine(other: FStream<U>): FStream<NTuple2<T, U>> {
        return sampleCombine(other = other, memory = false)
    }

    fun <U> sampleCombine(other: FStream<U>, memory: Boolean): FStream<NTuple2<T, U>> {
        val stream = FStream<NTuple2<T, U>>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        // keep track of last U. if this stream ends, we just hold on to the
        // last U forever.
        var lastU: U? = null
        val p1 = other.subscribeInner {
            val u = it
            if (u != null) {
                lastU = u
            }
        }
        // for every incoming value, combine the two
        val p2 = subscribeInner {
            val t = it
            if (t != null) {
                // only if we have a U
                val u = lastU
                if (u != null) {
                    inner.withValue {
                        it.update(NTuple2(t, u))
                    }
                }
            } else {
                inner.withValue {
                    it.update(null)
                }
            }
        }
        // keep both pegged
        stream.parent = Peg(pegs = mutableListOf(p1, p2))
        return stream
    }

    // Prepend a value to a stream. Or in other words, give a start value to a stream.
    fun startWith(value: T): FMemoryStream<T> {
        val stream = FMemoryStream<T>(memoryMode = MemoryMode.UntilEnd)
        val inner = stream.inner
        inner.withValue { it.update(value) }
        stream.parent = subscribeInner { t ->
            inner.withValue { it.update(t) }
        }
        return stream
    }

    // Take a fixed amount of elements, then end.
    fun take(amount: Int): FStream<T> {
        return take(amount = amount, memory = false)
    }

    fun take(amount: Int, memory: Boolean): FStream<T> {
        var todo = amount + 1
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        stream.parent = subscribeInner { t ->
            if (todo > 0) {
                todo -= 1
            }
            if (todo > 0) {
                inner.withValue { it.update(t) }
            }
            if (todo == 1) {
                inner.withValue { it.update(null) }
            }
        }
        return stream
    }

    // Take values from the stream while some condition hold true, then end the stream.
    fun takeWhile(f: (T) -> Boolean): FStream<T> {
        return takeWhile(f = f, memory = false)
    }

    fun takeWhile(f: (T) -> Boolean, memory: Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
        val inner = stream.inner
        stream.parent = subscribeInner {
            val t = it
            if (t != null) {
                if (f(t)) {
                    inner.withValue { it.update(t) }
                } else {
                    inner.withValue { it.update(null) }
                }
            } else {
                inner.withValue { it.update(null) }
            }
        }
        return stream
    }

    // Stall the thread until the stream to ends.
    // needs dummy return type to avoid name clash with java
    fun wait(): Boolean {
        val waitFor: Locker<Semaphore?> = Locker(value = Semaphore(0, true))
        val peg = remember().subscribeInner {
            if (it == null) {
                waitFor.withAndSetValue {
                    it?.release()
                    null
                }
            }
        }
        ignore(peg)
        val w = waitFor.withValue { it }
        w?.acquire()
        return true
    }

    fun beginScope(): FStream<T> {

        return this
    }

    fun endScope() {
        inner.withValue {
            it.update(t = null)
        }
    }

    override fun hashCode(): Int {
        var result = inner.hashCode()
        result = 31 * result + (parent?.hashCode() ?: 0)
        result = 31 * result + ident.hashCode()
        return result
    }

}

/// Flatten a stream of streams, sequentially. This means that any new stream
/// effectively interrupts the previous stream and we only get values from
/// the latest stream.
fun <T, U : FStream<T>> FStream<U>.flatten(): FStream<T> = this.flatten(memory = false)


fun <T, U : FStream<T>> FStream<U>.flatten(memory: Boolean): FStream<T> {
    val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val inner = stream.inner
    var currentIdent: Long = 0
    var outerEnded = false
    var peg: Peg? = null
    stream.parent = this.subscribeInner { nestedStream ->
        if (nestedStream != null) {
            if (currentIdent != nestedStream.ident) {
                // destroy the old peg
                peg?.destroy()
                peg = nestedStream.subscribeInner { t ->
                    if (t != null) {
                        inner.withValue { it.update(t) }
                    } else {
                        peg = null
                        currentIdent = 0
                        // the inner stream ending ends if the outer is ended
                        if (outerEnded) {
                            inner.withValue { it.update(null) }
                        }
                    }
                }
                currentIdent = nestedStream.ident
            }
        } else {
            outerEnded = true
            // the outer stream ending ends if inner is already ended, or not started
            if (peg == null) {
                inner.withValue { it.update(null) }
            }
        }
    }
    return stream
}

/// Flatten a stream of streams, concurrently. This means that any new stream
/// is just added to the current subscribed streams. We listen to all streams
/// coming.
fun <T, U : FStream<T>> FStream<U>.flattenConcurrently(): FStream<T> = flattenConcurrently(memory = false)

fun <T, U : FStream<T>> FStream<U>.flattenConcurrently(memory: Boolean): FStream<T> {
    val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val currentIdents: Locker<MutableList<Long>> = Locker(value = mutableListOf())
    val inner = stream.inner
    stream.parent = this.subscribeInner {
        val nestedStream = it
        if (nestedStream != null) {
            var peg: Peg? = null
            ignore(peg)
            val ident = nestedStream.ident
            // simply overwriting the old value will release the ARC
            peg = nestedStream.subscribeInner {
                val t = it
                if (t != null) {
                    inner.withValue { it.update(t) }
                } else {
                    // the inner stream ending does not end the result stream
                    peg?.destroy()
                    peg = null
                    currentIdents.withValue { it.removeAll { it == ident } }
                }
            }
            currentIdents.withValue { it.add(ident) }
        } else {
            // the outer stream ending does end the result stream
            inner.withValue { it.update(null) }
        }
    }
    return stream
}


// Merge a bunch of streams emitting the same T to one.
fun <T> merge(vararg streams: FStream<T>): FStream<T> {
    return merge(streams = *streams, memory = false)
}

fun <T> merge(vararg streams: FStream<T>, memory: Boolean): FStream<T> {
    val stream = FStream<T>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val inner = stream.inner
    // TODO a better strategy would be to unsubscribe from streams as they end
    var count = streams.size
    val pegs = streams.map { xstream ->
        xstream.subscribeInner {
            val t = it
            if (t != null) {
                inner.withValue { it.update(t) }
            } else {
                count -= 1
                // only end stream when all merged streams end
                if (count == 0) {
                    inner.withValue { it.update(null) }
                }
            }
        }
    }
    val mutablePegs = pegs.toMutableList()
    stream.parent = Peg(pegs = mutablePegs)
    return stream
}

// Specialization of FStream that has "memory". Memory means that any
// new listener added will straight away get the last value that went through the stream.
open class FMemoryStream<T> // We just inherit to make a clearer type to the user of this API.
    (memoryMode: MemoryMode) : FStream<T>(memoryMode = memoryMode) {
    // The actual implementation of this is entirely in the `Inner` class.

}

// The originator of a stream of values.
//
// ```
// let sink = FSink<Int>()
//
// let stream = sink.stream()
//
// ...
// sink.update(0)
// sink.update(1)
// sink.end()
// ```
class NullWrapper : Any()

class FSink<T>(memory: Boolean = false) {

    private var inner: Locker<Inner<T>> =
        Locker(value = Inner(if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))

    // Get a stream from this sink. Can be used multiple times and each instance
    // will be backed by the same sink.
    fun stream(): FStream<T> =
        FStream(inner = this.inner)

    // Update a value into the sink and all connected streams.  Inserting a null will
    //   cause a special NullWrapper class to be inserted instead.  This is due
    //   to the fact that a null entry in a stream is also used as a termination flag.
    fun update(t: T) {
        if (t == null) {
            this.inner.withValue {
                @Suppress("UNCHECKED_CAST")
                it.updateAndImitate(t = NullWrapper() as T)
            }
        } else {
            this.inner.withValue { it.updateAndImitate(t) }
        }
    }

    // End this sink. No more values can be sent after
    fun end() {
        this.inner.withValue { it.updateAndImitate(null) }
    }
}

// Helper to collect values from a stream. Mainly useful for tests.

class Collector<T> {
    private val inner: Locker<CollectorInner<T>> = Locker(value = CollectorInner())
    var parent: Peg? = null

    fun update(t: T?) {
        inner.withValue {
            if (it.alive) {
                if (t != null) {
                    it.values.add(t)
                } else {
                    it.waitFor.release()
                    it.alive = false
                }
            }
        }
    }

    // Stall the thread and wait for the stream this collector works off to end.
    fun wait(): MutableList<T> {
        val waitFor = this.inner.withValue {
            if (it.alive) {
                it.waitFor
            } else {
                null
            }
        }
        waitFor?.acquire()
        return take()
    }

    // Take whatever values are in the collector without waiting for
    // the stream to end.
    fun take(): MutableList<T> {
        return inner.withValue {
            val v = it.values
            it.values = mutableListOf()
            v
        }
    }
}


private data class CollectorInner<T>(
    var alive: Boolean = true,
    var values: MutableList<T> = mutableListOf(),
    val waitFor: Semaphore = Semaphore(0, true)
)

// Subscriptions are receipts to the `FStream.subscribe()` operation. They
// can be used to unsubscribe.
//
// ```
// let sink = FSink<Int>()
//
// let sub = sink.stream().subscribe() { print("\($0)") }
//
// sink.update(0)
// sub.unsubscribe()
// sink.update(1) // not received
// ```

private typealias SubscriptionId = Long
private val subscriptionCounter = AtomicLong(0)
// every subscription must survive GC until someone actively calls unsubscribe()
private val subscriptions = mutableMapOf<SubscriptionId, Subscription>()

class Subscription(strong: Strong<*>) {

    private val id: SubscriptionId = subscriptionCounter.getAndIncrement()

    private var strong: Strong<*>? = strong

    init {
        subscriptions[id] = this
    }

    // Set to true to automatically unsubscribe when the subscription deinits
    private var doUnsubscribeOnDeinit: Boolean = false

    // Unsubscribe from further updates.
    fun unsubscribe() {
        subscriptions.remove(id)
        strong?.clear()
        strong = null
    }

    @Suppress("unused")
    fun unsubscribeOnDeinit(): Subscription {
        doUnsubscribeOnDeinit = true
        subscriptions.remove(id)
        return this
    }

    protected fun finalize() {
        if (doUnsubscribeOnDeinit) {
            unsubscribe()
        }
    }
}

// Imitators are used to create cyclic streams. The imitator is an originator
// of a stream at the same time as it imitates some other stream further down
// the code.
//
// Here's a bad idea illustrating the usage:
// ```
// let imitator = FImitator<Int>() stream of int
//
// let x = imitator.stream().map() { $0 + 1 } // use imitator stream
// let y: FStream<Int> = ...
//
// let m = FStream.merge(x, y) // merge imiator with other stream
//
// imitator.imitate(m) // cycle all m up to imitator, this can only be done once
//
// // NB. This is a BAD IDEA, beacuse it causes an endless loop. FImitators must
// // be used with care to not spin out of control.
// ```
//

class FImitator<T> {
    var inner: Locker<Inner<T>>
    private var imitating = false

    @Suppress("ConvertSecondaryConstructorToPrimary")
    constructor(memory: Boolean = false) {
        inner = Locker(value = Inner((if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory)))
    }

    // Get a stream from this imitator. Can be used multiple times and each instance
    // will be backed by the same imitator.
    fun stream(): FStream<T> =
        FStream(inner = this.inner)

    // Start imitating another stream. This can be called exactly once.
    // Repeated calls will `fatalError`.
    //
    // Imitators create a cyclic dependency. The imitator will end if the
    // imitated stream ends, but if we want to break the cycle without
    // ending streams, the returned subscription is used.
    fun imitate(other: FStream<T>): Subscription {
        if (imitating) {
            throw Exception("imitate() used twice on the same imitator")
        }
        imitating = true
        return other.attachImitator(this)
    }
}

// Thread local collector of imitations that are to be done once the current stream
// invocation finishes. This is how we make sync imitations happen.
internal class ImitationThreadLocal : ThreadLocal<ImitationRunner>() {

    override fun initialValue() = ImitationRunner()

}

// Once a thread starts doing imitations we keep doing imitation at the same call site
// even when we encounter (possibly the same) imitation further down the call tree.
// Per thread, the first imitator to run sets "running = true" and if we encounter the
// running imitator, we don't start another in the same call stack, just append more todo
// for the same.
class ImitationRunner {
    var imitations = mutableListOf<Imitation>()
    var running = false
    fun append(imitation: Imitation) {
        imitations.add(imitation)
    }
    fun take(): MutableList<Imitation> {
        val l = imitations
        imitations = mutableListOf()
        return l
    }
}

private val imitations: Locker<ImitationThreadLocal> = Locker(value = ImitationThreadLocal())
typealias Imitation = () -> Unit

// Helper type to thread safely lock a value L. It is accessed via a closure.
class Locker<L>(private var value: L) {
    private val lock = ReentrantLock(true)
    // Access the locked in value
    fun <X> withValue(closure: (L) -> X): X {
        val x: X
        lock.lock()
        try {
            x = closure(value)
        } finally {
            lock.unlock()
        }
        return x
    }

    // apply a closure on the value and set the value to the result
    fun withAndSetValue(closure: (L) -> L): L {
        val x: L
        lock.lock()
        try {
            x = closure(value)
            value = x
        } finally {
            lock.unlock()
        }
        return x
    }
}

// The kinds of memory modes we have
enum class MemoryMode {
    NoMemory, UntilEnd, AfterEnd;

    // No memory, don't remember values
    // Remember values, but not after the stream ends
    // Remember values, also after the stream ends
    // Test if this has memory
    fun isMemory(): Boolean =
        this == UntilEnd || this == AfterEnd
}

// The inner type in a FStream that is protected via a Locker
class Inner<T>(var memoryMode: MemoryMode) {
    private var alive = true
    private var ws: MutableList<Weak<Listener<T?>>> = mutableListOf()
    private var ss: MutableList<Strong<Listener<T?>>> = mutableListOf()
    private var lastValue: T? = null

    // Weakly subscribe to values passing this instance
    fun subscribeWeak(onvalue: (T?) -> Unit): Peg {
        if (!alive) {
            if (memoryMode == MemoryMode.AfterEnd) {
                onvalue(lastValue)
            } else {
                onvalue(null)
            }
            return Peg(l = 0 as Any) // fake peg
        }
        val l = Listener(closure = onvalue)
        val w = Weak(value = l)
        val p = Peg(l = l)
        ws.add(w)
        if (memoryMode.isMemory() && lastValue != null) {
            onvalue(lastValue)
        }
        return p
    }

    // Strongly subscribe to values passing this instance
    fun subscribeStrong(peg: Peg?, onvalue: (T?) -> Unit): Strong<Listener<T?>> {
        if (!alive) {
            if (memoryMode == MemoryMode.AfterEnd) {
                onvalue(lastValue)
            } else {
                onvalue(null)
            }
            return Strong(value = null)
        }
        val l = Listener(closure = onvalue)
        l.extra = peg
        val s = Strong(value = l)
        ss.add(s)
        if (memoryMode.isMemory() && lastValue != null) {
            onvalue(lastValue)
        }
        return s
    }

    // Update that also runs imitators after the update finishes.
    fun updateAndImitate(t: T?) {
        // normal update
        update(t)
        // any imitator that have been gathered during the update is executed now
        // sync with the same update. keep doing this until there are no more
        // imitators added.
        var isDoingTheRunning = false
        while (true) {
            var todo: MutableList<Imitation> = mutableListOf()
            // this is inside a lock, we must get the value out and release
            // the lock since the imitator run might need the lock to add
            // more imitators (i.e. avoid deadlock).
            imitations.withValue {
                val imitationRunner = it.get() ?: throw Error("Bad thread local imitator state (null)")
                if (!imitationRunner.running || isDoingTheRunning) {
                    imitationRunner.running = true
                    isDoingTheRunning = true
                    todo = imitationRunner.take()
                }
            }
            if (todo.isEmpty()) {
                // nothing to do
                break
            } else {
                // run all imitators
                todo.forEach {
                    it()
                }
            }
        }
        if (isDoingTheRunning) {
            imitations.withValue {
                val imitationRunner = it.get() ?: throw Error("Bad thread local imitator state (null)")
                imitationRunner.running = false
            }
        }
    }

    // Update a new value to the stream. `nil` indicates the end of the stream
    fun update(t: T?) {
        if (!alive) {
            return
        }
        // strong subscribers get the value first, only
        // keep listeners that haven't unsubscribed
        ss = (ss.filter {
            val s = it.get()
            if (s == null) {
                false
            } else {
                s.apply(t = t)
                true
            }
        }) as MutableList<Strong<Listener<T?>>>
        // weak subscribers second, only keep the
        // ones that are still there.
        ws = (ws.filter {
            val w = it.get()
            if (w == null) {
                false
            } else {
                w.apply(t = t)
                true
            }
        }) as MutableList<Weak<Listener<T?>>>
        // nil indicates the end of the stream
        if (t == null) {
            alive = false
            // release all listeners
            ws.forEach {
                it.get()?.destroy()
            }
            ws = mutableListOf()
            ss.forEach {
                it.get()?.destroy()
            }
            ss = mutableListOf()
            if (memoryMode != MemoryMode.AfterEnd) {
                lastValue = null
            }
        } else {
            if (memoryMode.isMemory()) {
                lastValue = t
            }
        }
    }
}

// A listener is just a closure here wrapped in a class so
// we can in turn put it inside a `Weak` or `Strong`.

class Listener<T>(val closure: (T?) -> Unit) {
    // if we need to hold a reference to something more :)
    var extra: Any? = null
    private var valid = true // hack to allow disconnecting a listener

    fun apply(t: T?) {
        if (valid) {
            closure(t)
        }
    }

    fun destroy() {
        extra = null
        valid = false
    }
}

// Untyped strong reference to something. We use it to keep strong
// references to a `Listener<T>` so that we can weakly subscribe
// to a parent stream and make the lifetime of the ARC "live" in
// the child object.

data class Peg(
    var parent: Any? = null,
    var l: Any? = null
) {

    constructor(l: Any) : this() {
        this.l = l
    }

    constructor(pegs: MutableList<Peg>) : this() {
        l = pegs
    }

    fun destroy() {
        if (l != null) {
            if (l is MutableList<*>) {
                (l as MutableList<*>).forEach {
                    if (it is Peg) {
                        it.destroy()
                    }
                }
            } else if (l is Listener<*>) {
                (l as Listener<*>).destroy()
            }
        }
    }
}

// Protocol to make `Weak` and `Strong` behave the same.
private interface Get {
    fun get(): Any?
}

// Weak reference to some object
class Weak<W : Any>(value: W) : Get {
    private var value: WeakReference<W> = WeakReference(value)

    override fun get(): W? =
        value.get()

}

// Strong reference to some object
class Strong<W : Any>(var value: W?) : Get {

    override fun get(): W? =
        value

    fun clear() {
        value = null
    }
}

// Dummy function to let us ignore values that are not read.
@Suppress("UNUSED_PARAMETER")
fun <T> ignore(x: T) {
}

// Need tuples (Kotlin only offers Pairs and Triples
// The tuples are 'smart' about the special NullWrapper data type, replacing their values with null's
// NTuple1 is not used directly, but is base class of tuples, and is here just to ensure this backing property scheme
//   is inherited correctly
open class NTuple1<T>(private val _a: T?) {

    val a: T?
        get() {
            return if (NullWrapper::class.java.isInstance(_a)) {
                null
            } else _a
        }
}

open class NTuple2<T, U>(_a: T?, private val _b: U?) : NTuple1<T>(_a) {

    val b: U?
        get() {
            return if (NullWrapper::class.java.isInstance(_b)) {
                null
            } else _b
        }
}

open class NTuple3<T, U, V>(_a: T?, _b: U?, private val _c: V?) : NTuple2<T, U>(_a, _b) {

    val c: V?
        get() {
            return if (NullWrapper::class.run { java.isInstance(_c) }) {
                null
            } else _c
        }
}

open class NTuple4<T, U, V, W>(_a: T?, _b: U?, _c: V?, private val _d: W?) : NTuple3<T, U, V>(_a, _b, _c) {

    val d: W?
        get() {
            return if (NullWrapper::class.java.isInstance(_d)) {
                null
            } else _d
        }
}

open class NTuple5<T, U, V, W, X>(_a: T?, _b: U?, _c: V?, _d: W?, private val _e: X?) :
    NTuple4<T, U, V, W>(_a, _b, _c, _d) {

    @Suppress("unused")
    val e: X?
        get() {
            return if (NullWrapper::class.java.isInstance(_e)) {
                null
            } else _e
        }
}
//data class NTuple6<T,U,V,W,X,Y>(val a: T, val b: U, val c: V, val d: W, val e: X, val f: Y)

// MARK: ABANDON ALL HOPE YE WHO ENTERS HERE!
// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
fun <A, B> combine(a: FStream<A>, b: FStream<B>): FStream<NTuple2<A, B>> {
    return combine(a = a, b = b, memory = false)
}

fun <A, B> combine(a: FStream<A>, b: FStream<B>, memory: Boolean): FStream<NTuple2<A, B>> {
    val stream = FStream<NTuple2<A, B>>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    val emit = {
        inner.withValue {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    it.update(NTuple2(aa, bb))
                }
            }
        }
    }
    var count = 2
    val pegs: MutableList<Peg> = mutableListOf(
        a.subscribeInner {

            val t = it
            if (t != null) {
                va = t
                emit()
            } else {
                count -= 1
                if (count == 0) {
                    inner.withValue { it.update(null) }
                }
            }
        }, b.subscribeInner {
            val t = it
            if (t != null) {
                vb = t
                emit()
            } else {
                count -= 1
                if (count == 0) {
                    inner.withValue { it.update(null) }
                }
            }
        })
    stream.parent = Peg(pegs = pegs)
    return stream
}

// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
fun <A, B, C> combine(a: FStream<A>, b: FStream<B>, c: FStream<C>): FStream<NTuple3<A, B, C>> {
    return combine(a = a, b = b, c = c, memory = false)
}

fun <A, B, C> combine(a: FStream<A>, b: FStream<B>, c: FStream<C>, memory: Boolean): FStream<NTuple3<A, B, C>> {
    val stream = FStream<NTuple3<A, B, C>>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    var vc: C? = null
    val emit = {
        inner.withValue {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    val cc = vc
                    if (cc != null) {
                        it.update(NTuple3(aa, bb, cc))
                    }
                }
            }
        }
    }
    var count = 3
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, b.subscribeInner {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, c.subscribeInner {
        val t = it
        if (t != null) {
            vc = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}

// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
@Suppress("unused")
fun <A, B, C, D> combine(a: FStream<A>, b: FStream<B>, c: FStream<C>, d: FStream<D>): FStream<NTuple4<A, B, C, D>> {
    return combine(a = a, b = b, c = c, d = d, memory = false)
}

fun <A, B, C, D> combine(
    a: FStream<A>,
    b: FStream<B>,
    c: FStream<C>,
    d: FStream<D>,
    memory: Boolean
): FStream<NTuple4<A, B, C, D>> {
    val stream = FStream<NTuple4<A, B, C, D>>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    var vc: C? = null
    var vd: D? = null
    val emit = {
        inner.withValue {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    val cc = vc
                    if (cc != null) {
                        val dd = vd
                        if (dd != null) {
                            it.update(NTuple4(aa, bb, cc, dd))
                        }
                    }
                }
            }
        }
    }
    var count = 4
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, b.subscribeInner {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, c.subscribeInner {
        val t = it
        if (t != null) {
            vc = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, d.subscribeInner {
        val t = it
        if (t != null) {
            vd = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}

// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
@Suppress("unused")
fun <A, B, C, D, E> combine(
    a: FStream<A>,
    b: FStream<B>,
    c: FStream<C>,
    d: FStream<D>,
    e: FStream<E>
): FStream<NTuple5<A, B, C, D, E>> {
    return combine(a = a, b = b, c = c, d = d, e = e, memory = false)
}

fun <A, B, C, D, E> combine(
    a: FStream<A>,
    b: FStream<B>,
    c: FStream<C>,
    d: FStream<D>,
    e: FStream<E>,
    memory: Boolean
): FStream<NTuple5<A, B, C, D, E>> {
    val stream =
        FStream<NTuple5<A, B, C, D, E>>(memoryMode = (if (memory) MemoryMode.UntilEnd else MemoryMode.NoMemory))
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    var vc: C? = null
    var vd: D? = null
    var ve: E? = null
    val emit = {
        inner.withValue {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    val cc = vc
                    if (cc != null) {
                        val dd = vd
                        if (dd != null) {
                            val ee = ve
                            if (ee != null) {
                                it.update(NTuple5(aa, bb, cc, dd, ee))
                            }
                        }
                    }
                }
            }
        }
    }
    var count = 5
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, b.subscribeInner {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, c.subscribeInner {
        val t = it
        if (t != null) {
            vc = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, d.subscribeInner {
        val t = it
        if (t != null) {
            vd = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    }, e.subscribeInner {
        val t = it
        if (t != null) {
            ve = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}
