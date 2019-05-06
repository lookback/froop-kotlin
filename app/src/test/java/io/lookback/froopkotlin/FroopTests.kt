package io.lookback.froopkotlin

import org.junit.Test

import org.junit.Assert.*
import java.util.concurrent.Semaphore

//
//  froopTests.kt
//  froopTests
//
//  Created by bhicks on 2019-05-05
//
//  original froopTests.swift Created by martin on 2019-02-23.
//  Copyright © 2019 Lookback Ltd. All rights reserved.
//
@kotlin.ExperimentalUnsignedTypes
class FroopTests {

    @Test
    fun testLinkedChain() {
        // this way we get a scope that deallocates
        // arcs at the end of it. the chain should
        // survive it.
        fun makeLinked() : Pair<FSink<Int>, Collector<Int>> {
            val sink = FSink<Int>()

            val collect = sink.stream()
                .filter() { it % 2 == 0 } // there's a risk this intermediary drops
                .map() { it * 2 }
                .collect()

            return Pair(sink, collect)
        }


        val (sink, collect) = makeLinked()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(0, 4))
    }

    @Test
    fun testFSink() {
        val sink = FSink<Int>()
        val collect = sink.stream().collect();

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(0, 1, 2))
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

        var r = mutableListOf<Int>()
        val sub = sink.stream().subscribe() { r.add(it) }

        sink.update(0)
        sink.update(1)
        sub.unsubscribe()
        sink.update(2)

        assertEquals(r, mutableListOf(0, 1))
    }

    @Test
    fun testSubscribeLink() {
        // this way we get a scope that deallocates
        // arcs at the end of it. the chain should
        // survive it.
        fun makeLinked(waitFor: Semaphore) : FSink<Int> {
            val sink = FSink<Int>()

            sink.stream()
                .map() { it * 2 } // there's a risk this intermediary drops
                .subscribe() {  // this subscribe adds a strong listener, chain should live
                    waitFor.tryAcquire()
                }

            return sink
        }

        val waitFor = Semaphore(1)
        val sink = makeLinked(waitFor)

        sink.update(1)

        // if the chain dropped, this will just stall
        waitFor.acquire()
    }

    @Test
    fun testSubscribeEnd() {
        val sink = FSink<Int>()

        var r = mutableListOf<Int>()
        sink.stream().subscribeEnd() {
            r.add(42)
        }

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(r, mutableListOf(42))
    }

    @Test
    fun testFilter() {
        val sink = FSink<Int>()

        val filt = sink.stream().filter() { it % 2 == 0 }
        val collect = filt.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(0, 2))
    }

    @Test
    fun testFilterMap() {
        val sink = FSink<Int>()

        val filt = sink.stream().filterMap() { if (it % 2 == 0) "$it" else null }
        val collect = filt.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(collect.wait(), mutableListOf("0", "2"))
    }

    @Test
    fun testImitate() {
        val imitator = FImitator<Int>()
        val collect = imitator.stream().collect();

        val sink = FSink<Int>()
        val stream = sink.stream()

        imitator.imitate(other = stream)

        sink.update(0)
        sink.update(1)
        assertEquals(collect.take(), mutableListOf(0, 1))

        sink.update(2)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(2))
    }

    @Test
    fun testImitateDealloc() {
        // this way we get a scope that deallocates
        // arcs at the end of it. the chain should
        // survive it.
        fun makeLinked() : Pair<FSink<Int>, Collector<Int>> {
            val imitator = FImitator<Int>()
            val sink = FSink<Int>()

            val trans = imitator.stream().map() { it + 40 }

            val mer = merge(trans.take(amount = 1u), sink.stream())

            imitator.imitate(other = sink.stream())

            val collect = mer.collect()

            return Pair(sink, collect)
        }


        val (sink, collect) = makeLinked()

        sink.update(2)
        sink.end()

        assertEquals(collect.take(), mutableListOf(2, 42))
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

        assertEquals(collect.wait(), mutableListOf(0, 1, 2))
    }

//    @Test
//    fun testDedupeBy() {
//        class Foo(i: Int) { }
//
//        val sink = FSink<Foo>()
//
//        val deduped = sink.stream().dedupeBy() { it.i }
//        val collect = deduped
//                .map { it.i }
//            .collect()
//
//        sink.update(Foo(0))
//        sink.update(Foo(0))
//        sink.update(Foo(1))
//        sink.update(Foo(2))
//        sink.update(Foo(2))
//        sink.end()
//
//        assertEquals(collect.wait(), mutableListOf(0, 1, 2))
//    }

    @Test
    fun testDrop() {
        val sink = FSink<Int>()

        val dropped = sink.stream().drop(amount = 2u)
        val collect = dropped.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.update(3)
        sink.update(4)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(2, 3, 4))
    }

    @Test
    fun testDropWhile() {
        val sink = FSink<Int>()

        val dropped = sink.stream().dropWhile() { it % 2 == 1 }
        val collect = dropped.collect()

        sink.update(1)
        sink.update(3)
        sink.update(4)
        sink.update(5)
        sink.update(6)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(4, 5, 6))
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

        assertEquals(collect.wait(), mutableListOf(0, 1))
    }

    @Test
    fun testFold() {
        val sink = FSink<Int>()

        val folded = sink.stream().fold("|") { a, b -> "$a + $b" }
        val collect = folded.collect()

        sink.update(0)
        sink.update(1)
        sink.end()

        assertEquals(collect.wait(), mutableListOf("|", "| + 0", "| + 0 + 1"))
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

        assertEquals(collect.wait(), mutableListOf(2))
    }

    @Test
    fun testMap() {
        val sink = FSink<Int>()

        val mapped = sink.stream().map() { "$it" }
        val collect = mapped.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)
        sink.end()

        assertEquals(collect.wait(), mutableListOf("0", "1", "2"))
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

        assertEquals(collect.wait(), mutableListOf("42", "42", "42"))
    }

    @Test
    fun testRemember() {
        val sink = FSink<Int>()

        val rem = sink.stream().remember();

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
        assertEquals(r.map() {it}, mutableListOf(1, 2, 3))
//        assertEquals(r.map() {it is String}, mutableListOf("foo", "bar", "bar"))
    }

    @Test
    fun testStartWith() {
        val sink = FSink<Int>()

        val collect = sink.stream().startWith(value = 42).collect()

        sink.update(0)
        sink.update(1)
        sink.end()

        assertEquals(collect.wait(), mutableListOf(42, 0, 1))
    }

    @Test
    fun testTake() {
        val sink = FSink<Int>()

        val taken = sink.stream().take(amount = 2u)
        val collect = taken.collect()

        sink.update(0)
        sink.update(1)
        sink.update(2)

        assertEquals(collect.wait(), mutableListOf(0, 1))
    }

    @Test
    fun testTakeExact() {
        val sink = FSink<Int>()

        val taken = sink.stream().take(amount = 2u)
        val collect = taken.collect()

        sink.update(0)
        sink.update(1)

        assertEquals(collect.wait(), mutableListOf(0, 1))
    }

    @Test
    fun testTakeWhile() {
        val sink = FSink<Int>()

        val taken = sink.stream().takeWhile() { it >= 0 }
        val collect = taken.collect()

        sink.update(0)
        sink.update(1)
        sink.update(-2)

        assertEquals(collect.wait(), mutableListOf(0, 1))
    }

    @Test
    fun testEnd() {
        val sink = FSink<Int>()
        sink.update(0)

        sink.end()

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
        sink.end() // doesn't end outer, because sink2 is active
        sink2.update(4)
        sink2.end()

        assertEquals(collect.wait(), mutableListOf(1, 2, 3, 4))
    }

//    @Test
//    fun testFlattenSame() {
//        data class Foo(var stream: FStream<Int>, var other: Float { }
//
//        enum class FooUpdate {
//            stream : FStream<Int>,
//            other : Float
//        }
//
//        val sinkInt = FSink<Int>()
//        val sinkUpdate = FSink<FooUpdate>()
//
//        val foo = sinkUpdate.stream().fold(Foo(stream = never(), other = 0.0f))
//        { prev, upd ->
//            var next = prev
//            when (upd) {
//                FooUpdate.stream(val stream) ->
//                    next.stream = stream
//                FooUpdate.other(val other) ->
//                    next.other = other
//            }
//            return next
//        }
//
//        val intStream = flatten(nested = foo.map() { it.stream.remember() })
//        sinkUpdate.update(intStream.stream(sinkInt.stream()))
//        val coll = intStream.collect()
//
//        sinkInt.update(42)
//
//        // what we don't want to see is [42, 42 ,42] repeated for each update of Foo
//        sinkUpdate.update(intStream.other(1.0))
//        sinkUpdate.update(intStream.other(2.0))
//        sinkUpdate.update(intStream.other(3.0))
//
//        sinkInt.update(43)
//
//        sinkUpdate.end()
//        sinkInt.end()
//
//        assertEquals(coll.wait(), mutableListOf(42, 43))
//    }

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

        assertEquals(collect.wait(), mutableListOf(1, 2, 42, 3))
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

        assertEquals(collect.wait(), mutableListOf(0,10,1,11))
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
//        assertEquals(r.map() {it.0}, mutableListOf(0, 1, 1))
//        assertEquals(r.map() {it.1}, mutableListOf("0", "0", "1"))
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
//        assertEquals(r.map() {it.0}, mutableListOf(null, null))
//        assertEquals(r.map() {it.1}, mutableListOf(null, "hi"))
    }

    @Test
    fun testCombineFromSame() {
        val sink1 = FSink<Int>()
        val stream1 = sink1.stream()

        val stream2 = stream1.map() { "$it" }

        val comb = combine(stream1, stream2)

        val collect = comb.collect()

        sink1.update(1) // value from both
        sink1.end()

        val r = collect.wait()

        // swift tuples are not equatable?!
//        assertEquals(r.map() {it.0}, mutableListOf(1))
//        assertEquals(r.map() {it.1}, mutableListOf("1"))
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

        val r = mutableListOf(collect.wait())

//        assertEquals(r.map() {it.0}, mutableListOf(1))
//        assertEquals(r.map() {it.1}, mutableListOf(2))
//        assertEquals(r.map() {it.2}, mutableListOf(3))
    }

}
