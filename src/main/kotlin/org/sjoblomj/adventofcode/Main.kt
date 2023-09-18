package org.sjoblomj.adventofcode

import org.sjoblomj.adventofcode.day1.day1
import org.sjoblomj.adventofcode.day2.day2
import org.sjoblomj.adventofcode.day3.day3
import org.sjoblomj.adventofcode.day4.day4
import org.sjoblomj.adventofcode.day5.day5
import org.sjoblomj.adventofcode.day6.day6
import org.sjoblomj.adventofcode.day7.day7
import kotlin.system.measureTimeMillis

fun main() {
	runDay(1) { day1() }
	runDay(2) { day2() }
	runDay(3) { day3() }
	runDay(4) { day4() }
	runDay(5) { day5() }
	runDay(6) { day6() }
	runDay(7) { day7() }
}

private fun runDay(number: Int, day: () -> Any) {
	println("== DAY $number ==")
	val timeTaken = measureTimeMillis { day.invoke() }
	println("Finished day $number in $timeTaken ms\n")
}
