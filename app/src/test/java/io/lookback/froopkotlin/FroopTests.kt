package io.lookback.froopkotlin

import org.junit.Test

import org.junit.Assert.*
import java.util.concurrent.Semaphore
import kotlin.concurrent.thread

//
//  froopTests.kt
//  froopTests
//
//  Created by bhicks on 2019-05-05
//
//  original froopTests.swift Created by martin on 2019-02-23.
//  Copyright © 2019 Lookback Ltd. All rights reserved.
//
class FroopTests {

    @Test
    fun testLinkedChain() {
        // this way we get a scope that deallocates
        // arcs at the end of it. the chain should
        // survive it.
        fun makeLinked() : NTuple2<FSink<Int>,Collector<Int>> {
            val sink = FSink<Int>()

            val collect = sink.stream()
                .filter { it % 2 == 0 } // there's a risk this intermediary drops
                .map { it * 2 }
                .collect()

            return NTuple2(sink, collect)
        }

        val linked = makeLinked()

        linked.a.update(0)
        linked.a.update(1)
        linked.a.update(2)
        linked.a.end()

        assertEquals(mutableListOf(0, 4), linked.b.wait())
    }

    @Test
    fun testUnsubOutOfScope() {
        val sink = FSink<Int>()
        val values = mutableListOf<Int>()

        fun scoped(stream: FStream<Int>) {
            stream.map {
                values.add(it)
            }
            // because x "falls out of scope here", the Peg would
            // in a swift froop unsubscribe the .map() from stream.
            stream.endScope()
        }

        val stream = sink.stream()

        scoped(stream)

        sink.update(42)
        sink.end()

        assertEquals(mutableListOf<Int>(), values)
    }

    @Test
    fun testFSink() {
        val sink = FSink<Int>()
        val collect = sink.stream().collect()

        // do on another thread
        thread(start = true) {
            sink.update(0)
            sink.update(1)
            sink.update(2)
            sink.end()
        }

        assertEquals(mutableListOf(0, 1, 2), collect.wait())
    }

    //    fun testOf() {
    //        val of = FStream.of(value: 42)
    //        val c1 = of.collect();
    //        val c2 = of.collect();
    //        assertEquals(c1.take(), [42])
    //        assertEquals(c2.take(), [42])
    //    }

    //    fun testStartWithOf() {
    //
    //        val stream = FStream.of(value: 43)
    //        val collect = stream.startWith(value: 42).collect()
    //
    //        assertEquals(collect.take(), [43])
    //    }

    //    fun testMapMemory() {
    //        val i = FStream.of(value: 42)
    //            .mapTo(value: "Yo")
    //            .remember()
    //        val collect = i.take(amount: 1).collect()
    //        assertEquals(collect.wait(), ["Yo"])
    //    }

    @Test
    fun testSubscription() {
        val sink = FSink<Int>()

        val r = mutableListOf<Int>()
        val sub = sink.stream().subscribe { r.add(it) }

        sink.update(0)
        sink.update(1)
        sub.unsubscribe()
        sink.update(2)

        assertEquals(mutableListOf(0, 1), r)
    }

    @Test
    fun testSubscribeLink() {
        // this way we get a scope that deallocates
        // arcs at the end of it. the chain should
        // survive it.
        fun makeLinked(waitFor: Semaphore) : FSink<Int> {
            val sink = FSink<Int>()

            sink.stream()
                .map { it * 2 } // there's a risk this intermediary drops
                .subscribe {  // this subscribe adds a strong listener, chain should live
                    waitFor.release()
                }

            return sink
        }

        val waitFor = Semaphore(0)
        val sink = makeLinked(waitFor)

        sink.update(1)

        // if the chain dropped, this will just stall
        waitFor.acquire()
    }

    @Test
    fun testSubscribeEnd() {
        val sink = FSink<Int>()

        val r = mutableListOf<Int>()
        sink.stream().subscribeEnd {
            r.add(42)
        }

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf(42), r)
    }

    @Test
    fun testFilter() {
        val sink = FSink<Int>()

        val filt = sink.stream().filter { it % 2 == 0 }
        val collect = filt.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf(0, 2), collect.wait())
    }

    @Test
    fun testFilterMap() {
        val sink = FSink<Int>()

        val filt = sink.stream().filterMap { if (it % 2 == 0) "$it" else null }
        val collect = filt.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf("0", "2"), collect.wait())
    }

    @Test
    fun testImitate() {
        val imitator = FImitator<Int>()
        val collect = imitator.stream().collect()

        val sink = FSink<Int>()
        val stream = sink.stream()

        imitator.imitate(other = stream)

        sink.update(0)
        sink.update(1)
        assertEquals(mutableListOf(0, 1), collect.take())

        sink.update(2)
        sink.end()

        assertEquals(mutableListOf(2), collect.wait())
    }

    @Test
    fun testImitateDealloc() {
        // this way we get a scope that deallocates
        // arcs at the end of it. the chain should
        // survive it.
        fun makeLinked() : NTuple2<FSink<Int>,Collector<Int>> {
            val imitator = FImitator<Int>()
            val sink = FSink<Int>()

            val trans = imitator.stream().map { it + 40 }

            val mer = merge(trans.take(amount = 1), sink.stream())

            imitator.imitate(other = sink.stream())

            val collect = mer.collect()

            return NTuple2(sink, collect)
        }


        val linked = makeLinked()

        linked.a.update(2)
        linked.a.end()

        assertEquals(mutableListOf(2, 42), linked.b.take())
    }

    @Test
    fun testDedupe() {
        val sink = FSink<Int>()

        val deduped = sink.stream().dedupe()
        val collect = deduped.collect()

        sink.update(0)
        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.update(2)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf(0, 1, 2), collect.wait())
    }

    @Test
    fun testDedupeBy() {
        class Foo(val i: Int)

        val sink = FSink<Foo>()

        val deduped = sink.stream().dedupeBy { it.i }
        val collect = deduped
                .map { it.i }
            .collect()

        sink.update(Foo(0))
        sink.update(Foo(0))
        sink.update(Foo(1))
        sink.update(Foo(2))
        sink.update(Foo(2))
        sink.end()

        assertEquals(mutableListOf(0, 1, 2), collect.wait())
    }

    @Test
    fun testDrop() {
        val sink = FSink<Int>()

        val dropped = sink.stream().drop(amount = 2)
        val collect = dropped.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.update(3)
        sink.update(4)
        sink.end()

        assertEquals(mutableListOf(2, 3, 4), collect.wait())
    }

    @Test
    fun testDropWhile() {
        val sink = FSink<Int>()

        val dropped = sink.stream().dropWhile { it % 2 == 1 }
        val collect = dropped.collect()

        sink.update(1)
        sink.update(3)
        sink.update(4)
        sink.update(5)
        sink.update(6)
        sink.end()

        assertEquals(mutableListOf(4, 5, 6), collect.wait())
    }

    @Test
    fun testEndWhen() {
        val sink1 = FSink<Int>()
        val sink2 = FSink<String>()

        val ended = sink1.stream().endWhen(other = sink2.stream())
        val collect = ended.collect()

        sink1.update(0)
        sink2.update("ignored")
        sink1.update(1)
        sink2.end() // this ends "ended"
        sink1.update(2) // never seen

        assertEquals(mutableListOf(0, 1), collect.wait())
    }

    @Test
    fun testFold() {
        val sink = FSink<Int>()

        val folded = sink.stream().fold("|") { a, b -> "$a + $b" }
        val collect = folded.collect()

        sink.update(0)
        sink.update(1)
        sink.end()

        assertEquals(mutableListOf("|", "| + 0", "| + 0 + 1"), collect.wait())
    }

    @Test
    fun testLastValue() {
        val sink = FSink<Int>()

        val last = sink.stream().last()
        val collect = last.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf(2), collect.wait())
    }

    @Test
    fun testMap() {
        val sink = FSink<Int>()

        val mapped = sink.stream().map { "$it" }
        val collect = mapped.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf("0", "1", "2"), collect.wait())
    }

    @Test
    fun testMapTo() {
        val sink = FSink<Int>()

        val mapped = sink.stream().mapTo(value = "42")
        val collect = mapped.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(mutableListOf("42", "42", "42"), collect.wait())
    }

    @Test
    fun testRemember() {
        val sink = FSink<Int>()

        val rem = sink.stream().remember()

        sink.update(42)

        val c1 = rem.collect()

        sink.update(43)

        val c2 = rem.collect()

        sink.end()

        assertEquals(c1.wait(), mutableListOf(42, 43))
        assertEquals(c2.wait(), mutableListOf(43))
    }

    @Test
    fun testSampleCombine() {
        val sink1 = FSink<Int>()
        val sink2 = FSink<String>()

        val comb = sink1.stream().sampleCombine(sink2.stream())

        val collect = comb.collect()

        sink1.update(0) // ignored because no sink2 value
        sink2.update("foo")
        sink1.update(1)
        sink2.update("bar")
        sink1.update(2)
        sink2.end() // sink2 keeps "bar"
        sink1.update(3)

        sink1.end()

        val r = collect.wait()

        // swift tuples are not equatable?!
        assertEquals(r.map {it.a}, mutableListOf(1, 2, 3))
        assertEquals(r.map {it.b}, mutableListOf("foo", "bar", "bar"))
    }

    @Test
    fun testStartWith() {
        val sink = FSink<Int>()

        val collect = sink.stream().startWith(value = 42).collect()

        sink.update(0)
        sink.update(1)
        sink.end()

        assertEquals(mutableListOf(42, 0, 1), collect.wait())
    }

    @Test
    fun testTake() {
        val sink = FSink<Int>()

        val taken = sink.stream().take(amount = 2)
        val collect = taken.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)

        assertEquals(mutableListOf(0, 1), collect.wait())
    }

    @Test
    fun testTakeExact() {
        val sink = FSink<Int>()

        val taken = sink.stream().take(amount = 2)
        val collect = taken.collect()

        sink.update(0)
        sink.update(1)

        assertEquals(mutableListOf(0, 1), collect.wait())
    }

    @Test
    fun testTakeWhile() {
        val sink = FSink<Int>()

        val taken = sink.stream().takeWhile { it >= 0 }
        val collect = taken.collect()

        sink.update(0)
        sink.update(1)
        sink.update(-2)

        assertEquals(mutableListOf(0, 1), collect.wait())
    }

    @Test
    fun testEnd() {
        val sink = FSink<Int>()
        sink.update(0)

        // run the end on another thread!
        thread(start = true) {
            sink.end()
        }

        sink.stream().wait()
    }

    @Test
    fun testEndWhenEnded() {
        val sink = FSink<Int>()
        sink.update(0)
        sink.end()

        sink.stream().wait()
    }

    @Test
    fun testFlatten() {
        val sink: FSink<FStream<Int>> = FSink()

        val flat = flatten(nested = sink.stream())
        val collect = flat.collect()

        val sink1 = FSink<Int>()
        sink1.update(0) // missed
        sink.update(sink1.stream())
        sink1.update(1)
        sink1.update(2)
        val sink2 = FSink<Int>()
        sink.update(sink2.stream())
        sink1.update(42) // missed
        sink1.end() // does not end outer
        sink2.update(3)
        sink.end() // does end outer
        sink2.update(4)
        sink2.end()

        assertEquals(mutableListOf(1, 2, 3, 4), collect.wait())
    }

    @Test
    fun testFlattenMemory() {
        val sink: FSink<FStream<Int>> = FSink()

        val memoryStream = sink.stream().remember()

        val sink1 = FSink<Int>()
        sink1.update(0) // missed
        sink.update(sink1.stream())

        val flat = flatten(nested = memoryStream)
        val collect = flat.collect()

        // the memory$ first value is dispatched async, and there's no guarantee that
        // happens before we reach the update() rows below. this second ought to be
        // enough :)
        Thread.sleep(1000)

        sink1.update(1)
        sink1.update(2)

        sink.end() // doesn't end outer, because sink2 is active
        sink1.end()

        assertEquals(mutableListOf(1, 2), collect.wait())
    }


    @Test
    fun testFlattenSame() {
        data class Foo(var stream: FStream<Int>?, var other: Float?)

        data class FooUpdate(val fooType: Boolean, val stream: FStream<Int>? = null, val other: Float? = null)

        val sinkInt = FSink<Int>()
        val sinkUpdate = FSink<FooUpdate>()

        val fooStream = sinkUpdate.stream().fold(Foo(stream = FStream.never(), other = 0.0f))
        { prev, upd ->
            if (upd.fooType) {
                prev.stream = upd.stream
            } else {
                prev.other = upd.other
            }
            prev
        }

        val intStream = flatten(nested = fooStream.map { it.stream?.remember() } as FStream<FStream<Int>>)
        sinkUpdate.update(FooUpdate(true, stream = sinkInt.stream()))
        val collect = intStream.collect()

        sinkInt.update(42)

        // what we don't want to see is [42, 42 ,42] repeated for each update of Foo
        sinkUpdate.update(FooUpdate(false, other = 1.0f))
        sinkUpdate.update(FooUpdate(false, other = 2.0f))
        sinkUpdate.update(FooUpdate(false, other = 3.0f))

        sinkInt.update(43)

        sinkUpdate.end()
        sinkInt.end()

        assertEquals(mutableListOf(42, 43), collect.wait())

    }

    @Test
    fun testFlattenConcurrently() {
        val sink: FSink<FStream<Int>> = FSink()

        val flat = flattenConcurrently(nested = sink.stream())
        val collect = flat.collect()

        val sink1 = FSink<Int>()
        sink1.update(0) // missed
        sink.update(sink1.stream())
        sink1.update(1)
        sink1.update(2)
        val sink2 = FSink<Int>()
        sink.update(sink2.stream())
        sink1.update(42) // kept!
        sink1.end() // does not end outer
        sink2.update(3)
        sink.end() // does end outer
        sink2.update(4) // missed

        assertEquals(mutableListOf(1, 2, 42, 3), collect.wait())
    }

    @Test
    fun testMerge() {
        val sink1 = FSink<Int>()
        val sink2 = FSink<Int>()

        val merg = merge(sink1.stream(), sink2.stream())

        val collect = merg.collect()

        sink1.update(0)
        sink2.update(10)
        sink1.update(1)
        sink1.end() // doesnt end merge
        sink2.update(11)
        sink2.end()

        assertEquals(mutableListOf(0,10,1,11), collect.wait())
    }

    @Test
    fun testCombine() {
        val sink1 = FSink<Int>()
        val sink2 = FSink<String>()

        val comb = combine(sink1.stream(), sink2.stream())

        val collect = comb.collect()

        sink1.update(0) // nothing happens
        sink2.update("0") // first value
        sink1.update(1)
        sink2.update("1")
        sink1.end()
        sink2.end()

        val r = collect.wait()

        // swift tuples are not equatable?!
        assertEquals(mutableListOf(0, 1, 1), r.map {it.a})
        assertEquals(mutableListOf("0", "0", "1"), r.map {it.b})
    }

    @Test
    fun testCombineNull() {
        val sink1 = FSink<Int?>()
        val sink2 = FSink<String?>()

        val comb = combine(sink1.stream(), sink2.stream())

        val collect = comb.collect()

        sink1.update(null) // nothing happens
        sink2.update(null) // first value
        sink2.update("hi") // first value
        sink1.end()
        sink2.end()

        val r = collect.wait()

        // swift tuples are not equatable?!
        assertEquals(mutableListOf<Any?>(null, null), r.map {it.a})
        assertEquals(mutableListOf<Any?>(null, "hi"), r.map {it.b})
    }

    @Test
    fun testCombineFromSame() {
        val sink1 = FSink<Int>()
        val stream1 = sink1.stream()

        val stream2 = stream1.map { "$it" }

        val comb = combine(stream1, stream2)

        val collect = comb.collect()

        sink1.update(1) // value from both
        sink1.end()

        val r = collect.wait()

        // swift tuples are not equatable?!
        assertEquals(mutableListOf(1), r.map {it.a})
        assertEquals(mutableListOf("1"), r.map {it.b})
    }

    @Test
    fun testCombineWithEnding() {
        val sink1 = FSink<Int>()
        val sink2 = FSink<Int>()
        val sink3 = FSink<Int>()

        val comb = combine(sink1.stream(), sink2.stream(), sink3.stream())

        val collect = comb.collect()

        sink1.update(1)
        sink1.end()
        sink2.update(2)
        sink2.end()
        sink3.update(3)
        sink3.end()

        val r = collect.wait()

        assertEquals(mutableListOf(1), r.map {it.a})
        assertEquals(mutableListOf(2), r.map {it.b})
        assertEquals(mutableListOf(3), r.map {it.c})
    }

}
