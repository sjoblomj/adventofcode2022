package org.sjoblomj.adventofcode

import org.sjoblomj.adventofcode.day1.day1
import kotlin.system.measureTimeMillis

fun main() {
	runDay(1) { day1() }
}

private fun runDay(number: Int, day: () -> Any) {
	println("== DAY $number ==")
	val timeTaken = measureTimeMillis { day.invoke() }
	println("Finished day $number in $timeTaken ms\n")
}
