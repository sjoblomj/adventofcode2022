package org.sjoblomj.adventofcode.day6

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day6Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day6()
		readFileToTopic("src/main/resources/inputs/${day}.txt", d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("1361", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("3263", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}

	@Test
	fun `Can calculate total score`() {
		performTestAndAssertResult("mjqjpqmgbljsphdztnvjfqwrcgsmlb", 7 to 19)
		performTestAndAssertResult("bvwbjplbgvbhsrlpgdmjqwftvncz", 5 to 23)
		performTestAndAssertResult("nppdvjthqldpwncqszvftbrmjlhg", 6 to 23)
		performTestAndAssertResult("nznrnfrfntjfmvfwmzdfjlvtqnbhcprsg", 10 to 29)
		performTestAndAssertResult("zcfzfwzzqfrljwzlrfnpqdbhtmscgvjw", 11 to 26)
	}

	private fun performTestAndAssertResult(characters: String, expectedResults: Pair<Int, Int>) {
		val d = Day6()
		putDataOnTopic("test", characters, d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals(expectedResults.first.toString(), records.last { it.key() == "${day}$part1" }.value())
		assertEquals(expectedResults.second.toString(), records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}
}
