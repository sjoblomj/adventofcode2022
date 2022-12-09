package org.sjoblomj.adventofcode.day5

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day5Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day5()
		readFileToTopic("src/main/resources/inputs/${day}.txt", d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("QPJPLMNNR", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("BQDNWJPVJ", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}

	@Test
	fun `Can calculate total score`() {
		val testData = listOf(
			"    [D]    ",
			"[N] [C]    ",
			"[Z] [M] [P]",
			" 1   2   3 ",
			"",
			"move 1 from 2 to 1",
			"move 3 from 1 to 3",
			"move 2 from 2 to 1",
			"move 1 from 1 to 2"
		)

		val d = Day5()
		var index = 0
		testData.forEach { putDataOnTopic("${index++}", it, d.inputTopic) }

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("CMZ", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("MCD", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}
}
