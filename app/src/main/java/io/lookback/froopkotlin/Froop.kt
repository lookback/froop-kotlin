package io.lookback.froopkotlin

import java.lang.ref.WeakReference
import java.util.concurrent.Semaphore

//
//  port of froop.swift
//  Created by bhicks on 2019-05-03.
//  Copyright © 2019 Lookback Ltd. All rights reserved.
//
//  Original froop.swift Created by martin on 2019-02-23.
//
//  Global singelton that can redirect the froop logging from .debug()
var froopLog: (String, String) -> Unit = { label, message ->
    print("$label $message")
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
@kotlin.ExperimentalUnsignedTypes
open class FStream<T> {

    companion object {
        @kotlin.ExperimentalUnsignedTypes
        private var streamCount: Locker<ULong> = Locker(value = 0uL)

        // Create a stream that never emits anything. It stays inerts forever.
        @kotlin.ExperimentalUnsignedTypes
        fun <T> never(): FStream<T> {
            val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
            stream.inner.withValue() { it.update(null) }
            return stream
        }
    }

    val inner: Locker<Inner<T>>
    var parent: Peg? = null

    val ident: ULong = streamCount.withAndSetValue({
        it + 1uL
    })

    override operator fun equals(other: Any?): Boolean {
        if (other == null) {
            return false
        }

        if (other is FStream<*>) {
            var otherStream = other as? FStream<*>
            return ident == otherStream?.ident
        }
        return false
    }

    // a new stream with a new inner
    constructor(memoryMode: MemoryMode) {
        inner = Locker(value = Inner(memoryMode))
    }

    // a new stream with a cloned inner
    constructor(inner: Locker<Inner<T>>) {
        this.inner = inner
    }

    // Subscribe to values from this stream.
    fun subscribe(listener: (T) -> Unit): Subscription<T> =
        inner.withValue() {
            val strong = it.subscribeStrong(peg = parent) {
                if (it != null) {
                    listener(it)
                }
            }
            Subscription(strong)
        }

    // Subscribe to the end of the stream
    fun subscribeEnd(listener: () -> Unit): Subscription<T> =
        inner.withValue() {
            val strong = it.subscribeStrong(peg = parent) {
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
        var peg = inner.withValue() { it.subscribeWeak(onvalue = listener) }
        // This peg must also keep the parent stream alive.
        // This is for chained operators such as .map().filter().map()
        // where the intermediaries would be unsubscribed otherwise.
        peg.parent = parent as Any?
        return peg
    }

    // Collect values of this stream into an array of values.
    fun collect(): Collector<T> {
        val c = Collector<T>()
        c.parent = subscribeInner(listener = c.update)
        return c
    }

    // Print every object passing through this stream prefixed by the `label`.
    fun debug(label: String): FStream<T> =
        map() {
            froopLog(label, it.toString())
            it
        }

    // Dedupe the stream by extracting some equatable value from it.
    // The value is compared for consecutive elements.
    fun <U> dedupeBy(f: (T) -> U): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        var lastU: U? = null
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                val newU = f(t)
                if (lastU != newU) {
                    lastU = newU
                    inner.withValue() { it.update(t) }
                }
            } else {
                inner.withValue() { it.update(null) }
            }
        }
        return stream
    }

    // Drop a fixed number of initial values, then start emitting.

    fun drop(amount: ULong): FStream<T> {
        var todo = amount + 1uL
        return dropWhile() { _ ->
            if (todo > 0uL) {
                todo -= 1uL
            }
            todo > 0uL
        }
    }

    // Drop values while some condition holds true, then start emitting.
    // Once started emitting, it will never drop again.
    fun dropWhile(f: (T) -> Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        var dropping = true
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                if (dropping) {
                    dropping = f(t)
                }
                if (!dropping) {
                    inner.withValue() { it.update(t) }
                }
            } else {
                inner.withValue() { it.update(null) }
            }
        }
        return stream
    }

    // Make a stream that ends when some other stream ends.
    fun <U> endWhen(other: FStream<U>): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        val p1 = subscribeInner() { t ->
            // regular values or end, both are propagated
            inner.withValue() { it.update(t) }
        }
        val p2 = other.subscribeInner() {
            // ignore regular values, just look out for the end
            if (it == null) {
                inner.withValue() { it.update(null) }
            }
        }
        // peg both parent and other
        stream.parent = Peg(pegs = mutableListOf(p1, p2))
        return stream
    }

    // Filter the stream using some sort of test.
    fun filter(f: (T) -> Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                if (f(t)) {
                    inner.withValue() {
                        it.update(t)
                    }
                }
            } else {
                inner.withValue() {
                    it.update(null)
                }
            }
        }
        return stream
    }

    // Filter the stream and also transform the value.
    fun <U> filterMap(f: (T) -> U?): FStream<U> {
        val stream = FStream<U>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                val u = f(t)
                if (u != null) {
                    inner.withValue() {
                        it.update(u)
                    }
                }
            } else {
                inner.withValue() {
                    it.update(null)
                }
            }
        }
        return stream
    }

    // Fold the stream by combining values from the past with the new value.
    // The seed is emitted as the first value.
    //
    // This is roughly equivalent of an `Array.reduce()` or `Array.fold()`.
    fun <U> fold(seed: U, f: (U, T) -> U): FMemoryStream<U> {
        val stream = FMemoryStream<U>(memoryMode = MemoryMode.UntilEnd)
        val inner = stream.inner
        // emit seed as first value
        inner.withValue() { it.update(seed) }
        // keep track of previous value
        var prev = seed
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                val next = f(prev, t)
                prev = next
                inner.withValue() {
                    it.update(next)
                }
            } else {
                inner.withValue() {
                    it.update(null)
                }
            }
        }
        return stream
    }

    // Internal function that starts an imitator.
    fun attachImitator(imitator: FImitator<T>): Subscription<T> {
        val inner = imitator.inner
        return inner.withValue() {
            val strong = it.subscribeStrong(peg = parent) { t ->
                // an imitation is a "todo" closure that captures the value to be
                // dispatched later into the imitator. the todo is added to a
                // thread local and is called later, after the current evaluation
                // finishes.
                val todo: Imitation = {
                    inner.withValue() {
                        it.update(t)
                    }
                }
                imitations.withValue() {
                    // it is a ThreadLocal whose value is a list of Imitations
                    // add this todo to the list
                    it.get().add(todo)
                }
            }
            Subscription(strong)
        }
    }

    // Makes a stream that only emits the last value.
    fun last(): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        var lastValue: T? = null
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                lastValue = t
            } else {
                inner.withValue() {
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
        val stream = FStream<U>(memoryMode = MemoryMode.NoMemory)
        // We can't use "stream" inside the closure since that would capture the stream instance
        // and thus also the "peg" described above (we would get a cyclic dependency keeping the
        // tree alive). So we get an ARC reference to the inner, which is used to dispatch values.
        val inner = stream.inner
        // We subscribe to self and back comes the "peg" that goes into the new stream.
        stream.parent = subscribeInner() {
            // If it is a value, transform it otherwise end.
            val t = it
            if (t != null) {
                inner.withValue() { it.update(f(t)) }
            } else {
                inner.withValue() { it.update(null) }
            }
        }
        return stream
    }

    // Transform any incoming value to one fixed value.
    fun <U> mapTo(value: U): FStream<U> {
        val stream = FStream<U>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        stream.parent = subscribeInner() {
            if (it != null) {
                inner.withValue() { it.update(value) }
            } else {
                inner.withValue() { it.update(null) }
            }
        }
        return stream
    }

    // Make a stream that remembers the last value. Any new (or combinator) will get the
    // last emitted value straight away.
    fun remember(): FMemoryStream<T> {
        val stream = FMemoryStream<T>(memoryMode = MemoryMode.UntilEnd)
        val inner = stream.inner
        stream.parent = subscribeInner() { t ->
            inner.withValue() {
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
    fun <U> sampleCombine(other: FStream<U>): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        // keep track of last U. if this stream ends, we just hold on to the
        // last U forever.
        var lastU: U? = null
        val p1 = other.subscribeInner() {
            val u = it
            if (u != null) {
                lastU = u
            }
        }
        // for every incoming value, combine the two
        val p2 = subscribeInner() {
            val t = it
            if (t != null) {
                // only if we have a U
                val u = lastU
                if (u != null) {
                    inner.withValue() {
                        it.update(t)
                        it.update(u as T?)
                    }
                }
            } else {
                inner.withValue() {
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
        inner.withValue() { it.update(value) }
        stream.parent = subscribeInner() { t ->
            inner.withValue() { it.update(t) }
        }
        return stream
    }

    // Take a fixed amount of elements, then end.

    fun take(amount: UInt): FStream<T> {
        var todo = amount + 1u
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        stream.parent = subscribeInner() { t ->
            if (todo > 0u) {
                todo -= 1u
            }
            if (todo > 0u) {
                inner.withValue() { it.update(t) }
            }
            if (todo == 1u) {
                inner.withValue() { it.update(null) }
            }
        }
        return stream
    }

    // Take values from the stream while some condition hold true, then end the stream.
    fun takeWhile(f: (T) -> Boolean): FStream<T> {
        val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
        val inner = stream.inner
        stream.parent = subscribeInner() {
            val t = it
            if (t != null) {
                if (f(t)) {
                    inner.withValue() { it.update(t) }
                } else {
                    inner.withValue() { it.update(null) }
                }
            } else {
                inner.withValue() { it.update(null) }
            }
        }
        return stream
    }

    // Stall the thread until the stream to ends.
    // needs dummy return type to avoid name clash with java
    fun wait(): Boolean {
        val waitFor: Locker<Semaphore?> = Locker(value = Semaphore(0, true))
        val peg = remember().subscribeInner() {
            if (it == null) {
                waitFor.withAndSetValue() {
                    it?.release()
                    null
                }
            }
        }
        ignore(peg)
        val w = waitFor.withValue() { it }
        w?.acquire()
        return true
    }

}

// Dedupe the stream by the value in the stream itself
@kotlin.ExperimentalUnsignedTypes
fun <T> FStream<T>.dedupe(): FStream<T> {
    return dedupeBy({ t: T -> t })
}

// Flatten a stream of streams, sequentially. This means that any new stream
// effectively interrupts the previous stream and we only get values from
// the latest stream.
//
// Swift doesn't do recursive types, so we can't make an extension for
@kotlin.ExperimentalUnsignedTypes
fun <T> flatten(nested: FStream<FStream<T>>): FStream<T> {
    val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
    val inner = stream.inner
    var currentIdent: ULong = 0uL
    var outerEnded = false
    var peg: Peg? = null
    ignore(peg)
    stream.parent = nested.subscribeInner() {
        val nestedStream = it
        if (nestedStream != null) {
            if (currentIdent != nestedStream.ident) {
                // simply overwriting the old value will release the ARC
                peg = nestedStream.subscribeInner() {
                    val t = it
                    if (t != null) {
                        inner.withValue() { it.update(t) }
                    } else {
                        peg = null
                        currentIdent = 0uL
                        // the inner stream ending ends if the outer is ended
                        if (outerEnded) {
                            inner.withValue() { it.update(null) }
                        }
                    }
                }
                currentIdent = nestedStream.ident
            }
        } else {
            outerEnded = true
            // the outer stream ending ends if inner is already ended, or not started
            if (peg == null) {
                inner.withValue() { it.update(null) }
            }
        }
    }
    return stream
}

// Flatten a stream of streams, concurrently. This means that any new stream
// is just added to the current subscribed streams. We listen to all streams
// coming.
//
// Swift doesn't do recursive types, so we can't make an extension for 
@kotlin.ExperimentalUnsignedTypes
fun <T> flattenConcurrently(nested: FStream<FStream<T>>): FStream<T> {
    val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
    val currentIdents: Locker<MutableList<ULong>> = Locker(value = mutableListOf())
    val inner = stream.inner
    stream.parent = nested.subscribeInner() {
        val nestedStream = it
        if (nestedStream != null) {
            var peg: Peg? = null
            ignore(peg)
            val ident = nestedStream.ident
            // simply overwriting the old value will release the ARC
            peg = nestedStream.subscribeInner() {
                val t = it
                if (t != null) {
                    inner.withValue() { it.update(t) }
                } else {
                    // the inner stream ending does not end the result stream
                    peg = null
                    currentIdents.withValue() { it.removeAll() { it == ident } }
                }
            }
            currentIdents.withValue() { it.add(ident) }
        } else {
            // the outer stream ending does end the result stream
            inner.withValue() { it.update(null) }
        }
    }
    return stream
}

// Merge a bunch of streams emitting the same T to one.
@kotlin.ExperimentalUnsignedTypes
fun <T> merge(vararg streams: FStream<T>): FStream<T> {
    val stream = FStream<T>(memoryMode = MemoryMode.NoMemory)
    val inner = stream.inner
    // TODO a better strategy would be to unsubscribe from streams as they end
    var count = streams.size
    val pegs = streams.map() { xstream ->
        xstream.subscribeInner() {
            val t = it
            if (t != null) {
                inner.withValue() { it.update(t) }
            } else {
                count -= 1
                // only end stream when all merged streams end
                if (count == 0) {
                    inner.withValue() { it.update(null) }
                }
            }
        }
    }
    var mutablePegs = pegs.toMutableList()
    stream.parent = Peg(pegs = mutablePegs)
    return stream
}

// Specialization of FStream that has "memory". Memory means that any
// new listener added will straight away get the last value that went through the stream.
@kotlin.ExperimentalUnsignedTypes
class FMemoryStream<T> : FStream<T> {
    // The actual implementation of this is entirely in the `Inner` class.

    // We just inherit to make a clearer type to the user of this API.
    constructor(memoryMode: MemoryMode) : super(memoryMode = memoryMode) {}
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
@kotlin.ExperimentalUnsignedTypes
class FSink<T> {
    private var inner: Locker<Inner<T>> = Locker(value = Inner(MemoryMode.NoMemory))

    // Get a stream from this sink. Can be used multiple times and each instance
    // will be backed by the same sink.
    fun stream(): FStream<T> =
        FStream(inner = inner)

    // Update a value into the sink and all connected streams.
    fun update(t: T) {
        inner.withValue() { it.updateAndImitate(t) }
    }

    // End this sink. No more values can be sent after 
    fun end() {
        inner.withValue() { it.updateAndImitate(null) }
    }
}

// Helper to collect values from a stream. Mainly useful for tests.
@kotlin.ExperimentalUnsignedTypes
class Collector<T> {
    private val inner: Locker<CollectorInner<T>>
    var parent: Peg? = null

    constructor() {
        inner = Locker(value = CollectorInner<T>())
    }

    fun update(t: T? = null) {
        inner.withValue() {
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

    val update: (T?) -> Unit = { update(null) }

    // Stall the thread and wait for the stream this collector works off to end.
    fun wait(): MutableList<T> {
        var waitFor = inner.withValue() {
            if (it.alive) {
                it.waitFor
            } else {
                null
            }
        }
        if (waitFor != null) {
            waitFor.acquire()
        }
        return take()
    }

    // Take whatever values are in the collector without waiting for
    // the stream to end.
    fun take(): MutableList<T> =
        inner.withValue() {
            val v = it.values
            it.values = mutableListOf()
            v
        }
}

@kotlin.ExperimentalUnsignedTypes
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
@kotlin.ExperimentalUnsignedTypes
class Subscription<T> : AutoCloseable {

    private var strong: Strong<Listener<T>>? = null
    // Set to true to automatically unsubscribe when the subscription deinits
    var unsubscribeOnDeinit: Boolean = false

    constructor(strong: Strong<Listener<T>>) {
        this.strong = strong
    }

    // Unsubscribe from further updates.
    fun unsubscribe() {
        strong?.clear()
        strong = null
    }

    override fun close() {
        if (unsubscribeOnDeinit) {
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
@kotlin.ExperimentalUnsignedTypes
class FImitator<T> {
    var inner: Locker<Inner<T>> = Locker(value = Inner(MemoryMode.NoMemory))
    private var imitating = false

    // Get a stream from this imitator. Can be used multiple times and each instance
    // will be backed by the same imitator.
    fun stream(): FStream<T> =
        FStream<T>(inner = inner)

    // Start imitating another stream. This can be called exactly once.
    // Repeated calls will `fatalError`.
    //
    // Imitators create a cyclic dependency. The imitator will end if the
    // imitated stream ends, but if we want to break the cycle without
    // ending streams, the returned subscription is used.
    fun imitate(other: FStream<T>): Subscription<T> {
        if (imitating) {
            throw Exception("imitate() used twice on the same imitator")
        }
        imitating = true
        return other.attachImitator(this)
    }
}

// Thread local collector of imitations that are to be done once the current stream
// invocation finishes. This is how we make sync imitations happen.
//
// Amazingly ThreadLocal is not thread safe, so we are forced to wrap it in a locker.
@kotlin.ExperimentalUnsignedTypes
private val imitations: Locker<ThreadLocal<MutableList<Imitation>>> = Locker(value = ThreadLocal())
typealias Imitation = () -> Unit

// Helper type to thread safely lock a value L. It is accessed via a closure.
@kotlin.ExperimentalUnsignedTypes
class Locker<L>(value: L) {
    private val semaphore = Semaphore(1, true)
    private var value = value;
    // Access the locked in value
    fun <X> withValue(closure: (L) -> X): X {
        semaphore.acquire()
        val x = closure(value)
        semaphore.release()
        return x
    }
    // apply a closure on the value and set the value to the result
    fun withAndSetValue(closure: (L) -> L): L {
        semaphore.acquire()
        value = closure(value)
        semaphore.release()
        return value
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
@kotlin.ExperimentalUnsignedTypes
class Inner<T>(memoryMode: MemoryMode) {
    var alive = true
    var ws: MutableList<Weak<Listener<T>>> = mutableListOf()
    var ss: MutableList<Strong<Listener<T>>> = mutableListOf()
    var memoryMode: MemoryMode = memoryMode
    var lastValue: T? = null

    // Weakly subscribe to values passing this instance
    fun subscribeWeak(onvalue: (T?) -> Unit): Peg {
        if (!alive) {
            if (memoryMode == MemoryMode.AfterEnd) {
                onvalue(lastValue)
            } else {
                onvalue(null)
            }
            return Peg(l = 0 as Any)
        }
        // fake peg
        val l = Listener(closure = onvalue)
        val w = Weak(value = l)
        val p = Peg(l = l)
        ws.add(w as Weak<Listener<T>>)
        if (memoryMode.isMemory() && lastValue != null) {
            onvalue(lastValue)
        }
        return p
    }

    // Strongly subscribe to values passing this instance
    fun subscribeStrong(peg: Peg?, onvalue: (T?) -> Unit): Strong<Listener<T>> {
        if (!alive) {
            if (memoryMode == MemoryMode.AfterEnd) {
                onvalue(lastValue)
            } else {
                onvalue(null)
            }
            return Strong(value = null)
        }
        val l = Listener(closure = onvalue)
        l.extra = peg as Any?
        val s = Strong(value = l)
        if (memoryMode.isMemory() && lastValue != null) {
            onvalue(lastValue)
        }
        ss.add(s as Strong<Listener<T>>)
        return s
    }

    // Update that also runs imitators after the update finishes.
    fun updateAndImitate(t: T?) {
        // normal update
        update(t)
        // any imitator that have been gathered during the update is executed now
        // sync with the same update. keep doing this until there are no more
        // imitators added.
        while (true) {
            var todo: MutableList<Imitation>? = mutableListOf()
            // this is inside a lock, we must get the value out and release
            // the lock since the imitator run might need the lock to add
            // more imitators (i.e. avoid deadlock).
            imitations.withValue() {
                todo = it.get() // save the list
                it.set(mutableListOf())// clear the list
            }
            if (todo == null) {
                break
            }
            if (todo!!.isEmpty()) {
                // nothing to do
                break
            } else {
                // run all imitators
                todo?.forEach() { it() }
            }
        }
    }

    // Update a new value to the stream. `nil` indicates the end of the stream
    fun update(t: T?): Boolean {
        if (!alive) {
            return false
        }
        // strong subscribers get the value first, only
        // keep listeners that haven't unsubscribed
        ss = (ss.filter() {
            val s = it.get() ?: return false
            s.apply(t = t)
            true
        }) as MutableList<Strong<Listener<T>>>
        // weak subscribers second, only keep the
        // ones that are still there.
        ws = (ws.filter() {
            val w = it.get() ?: return false
            w.apply(t = t)
            true
        }) as MutableList<Weak<Listener<T>>>
        // nil indicates the end of the stream
        if (t == null) {
            alive = false
            // release all listeners
            ws = mutableListOf()
            ss = mutableListOf()
            if (memoryMode != MemoryMode.AfterEnd) {
                lastValue = null
            }
        } else {
            if (memoryMode.isMemory()) {
                lastValue = t
            }
        }
        return true
    }
}

// A listener is just a closure here wrapped in a class so
// we can in turn put it inside a `Weak` or `Strong`.
@kotlin.ExperimentalUnsignedTypes
class Listener<T>(closure: (T?) -> Unit) {
    val closure = closure
    // if we need to hold a reference to something more :)
    var extra: Any? = null

    fun apply(t: T?) {
        closure(t)
    }
}

// Untyped strong reference to something. We use it to keep strong
// references to a `Listener<T>` so that we can weakly subscribe
// to a parent stream and make the lifetime of the ARC "live" in
// the child object.
@kotlin.ExperimentalUnsignedTypes
data class Peg(
    var parent: Any? = null,
    var l: Any? = null ) {

    constructor(l: Any) : this() {
        this.l = l
    }

    constructor(pegs: MutableList<Peg>) : this() {
        l = pegs
    }
}

// Protocol to make `Weak` and `Strong` behave the same.
private interface Get {
    fun get(): Any?
}

// Weak reference to some object
@kotlin.ExperimentalUnsignedTypes
class Weak<W : Any> : Get {
    var value: WeakReference<W?>

    constructor(value: W) {
        this.value = WeakReference(value)
    }

    override fun get(): W? =
        value.get()
}

// Strong reference to some object
@kotlin.ExperimentalUnsignedTypes
class Strong<W : Any> : Get {
    var value: W? = null

    constructor(value: W?) {
        this.value = value
    }

    override fun get(): W? =
        value

    fun clear() {
        value = null
    }
}

// Dummy function to let us ignore values that are not read.
@Suppress("UNUSED_PARAMETER")
fun <T> ignore(x: T) {}

// MARK: ABANDON ALL HOPE YE WHO ENTERS HERE!
// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
@kotlin.ExperimentalUnsignedTypes
fun <A, B> combine(a: FStream<A>, b: FStream<B>): FStream<Any> {
    val stream = FStream<Any>(memoryMode = MemoryMode.NoMemory)
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    val emit = {
        inner.withValue() {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    it.update(aa)
                    it.update(bb)
                }
            }
        }
    }
    var count = 2
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner() {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, b.subscribeInner() {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}

// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
@kotlin.ExperimentalUnsignedTypes
fun <A, B, C> combine(a: FStream<A>, b: FStream<B>, c: FStream<C>): FStream<Any> {
    val stream = FStream<Any>(memoryMode = MemoryMode.NoMemory)
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    var vc: C? = null
    val emit = {
        inner.withValue() {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    val cc = vc
                    if (cc != null) {
                        it.update(aa)
                        it.update(bb)
                        it.update(cc)
                    }
                }
            }
        }
    }
    var count = 3
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner() {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, b.subscribeInner() {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, c.subscribeInner() {
        val t = it
        if (t != null) {
            vc = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}

// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
@kotlin.ExperimentalUnsignedTypes
fun <A, B, C, D> combine(a: FStream<A>, b: FStream<B>, c: FStream<C>, d: FStream<D>): FStream<Any> {
    val stream = FStream<Any>(memoryMode = MemoryMode.NoMemory)
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    var vc: C? = null
    var vd: D? = null
    val emit = {
        inner.withValue() {
            val aa = va
            if (aa != null) {
                val bb = vb
                if (bb != null) {
                    val cc = vc
                    if (cc != null) {
                        val dd = vd
                        if (dd != null) {
                            it.update(aa)
                            it.update(bb)
                            it.update(cc)
                            it.update(dd)
                        }
                    }
                }
            }
        }
    }
    var count = 4
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner() {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, b.subscribeInner() {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, c.subscribeInner() {
        val t = it
        if (t != null) {
            vc = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, d.subscribeInner() {
        val t = it
        if (t != null) {
            vd = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}

// Combine a number of streams and emit values when any of them emit a value.
//
// All streams must have had at least one value before anything happens.
@kotlin.ExperimentalUnsignedTypes
fun <A, B, C, D, E> combine(a: FStream<A>, b: FStream<B>, c: FStream<C>, d: FStream<D>, e: FStream<E>): FStream<Any> {
    val stream = FStream<Any>(memoryMode = MemoryMode.NoMemory)
    val inner = stream.inner
    var va: A? = null
    var vb: B? = null
    var vc: C? = null
    var vd: D? = null
    var ve: E? = null
    val emit = {
        inner.withValue() {
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
                                it.update(aa)
                                it.update(bb)
                                it.update(cc)
                                it.update(dd)
                                it.update(ee)
                            }
                        }
                    }
                }
            }
        }
    }
    var count = 5
    val pegs: MutableList<Peg> = mutableListOf(a.subscribeInner() {
        val t = it
        if (t != null) {
            va = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, b.subscribeInner() {
        val t = it
        if (t != null) {
            vb = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, c.subscribeInner() {
        val t = it
        if (t != null) {
            vc = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, d.subscribeInner() {
        val t = it
        if (t != null) {
            vd = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    }, e.subscribeInner() {
        val t = it
        if (t != null) {
            ve = t
            emit()
        } else {
            count -= 1
            if (count == 0) {
                inner.withValue() { it.update(null) }
            }
        }
    })
    stream.parent = Peg(pegs = pegs)
    return stream
}
