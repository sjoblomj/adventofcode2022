package org.sjoblomj.adventofcode.day4

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day4Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day4()
		readFileToTopic("src/main/resources/inputs/${day}.txt", d.inputTopic)

		val stream = d.solve()

		Thread.sleep(10_000)
		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertTrue(records.any { it.key() == "${day}$part1" && it.value() == "503" })
		assertTrue(records.any { it.key() == "${day}$part2" && it.value() == "827" })
		stream.close()
	}

	@Test
	fun `Can calculate total score`() {
		val testData = listOf(
			"2-4,6-8",
			"2-3,4-5",
			"5-7,7-9",
			"2-8,3-7",
			"6-6,4-6",
			"2-6,4-8",
			"8-91,8-91",
		)

		val d = Day4()
		var index = 0
		testData.forEach { putDataOnTopic("${index++}", it, d.inputTopic) }

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("3", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("5", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}
}
