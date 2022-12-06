package org.sjoblomj.adventofcode

import org.sjoblomj.adventofcode.day1.day1
import org.sjoblomj.adventofcode.day2.day2
import org.sjoblomj.adventofcode.day3.day3
import kotlin.system.measureTimeMillis

fun main() {
	runDay(1) { day1() }
	runDay(2) { day2() }
	runDay(3) { day3() }
}

private fun runDay(number: Int, day: () -> Any) {
	println("== DAY $number ==")
	val timeTaken = measureTimeMillis { day.invoke() }
	println("Finished day $number in $timeTaken ms\n")
}
