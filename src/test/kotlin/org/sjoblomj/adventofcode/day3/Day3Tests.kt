package org.sjoblomj.adventofcode.day3

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day3Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day3()
		readFileToTopic("src/main/resources/inputs/${day}.txt", d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("8088", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("2522", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}

	@Test
	fun `Can calculate total score`() {
		val testData = listOf(
			"vJrwpWtwJgWrhcsFMMfFFhFp",
			"jqHRNqRjqzjGDLGLrsFMfFZSrLrFZsSL",
			"PmmdzqPrVvPwwTWBwg",
			"wMqvLMZHhHMvwLHjbvcjnnSBnvTQFn",
			"ttgJtRGJQctTZtZT",
			"CrZsJsPPZsGzwwsLwLmpwMDw"
		)

		val d = Day3()
		var index = 0
		testData.forEach { putDataOnTopic("${index++}", it, d.inputTopic) }

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("157", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("70", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}
}
