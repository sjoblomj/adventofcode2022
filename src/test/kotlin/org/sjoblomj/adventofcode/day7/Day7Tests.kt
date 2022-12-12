package org.sjoblomj.adventofcode.day7

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day7Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day7()
		readFileToTopic("src/main/resources/inputs/${day}.txt", d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("1444896", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("404395", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}

	@Test
	fun `Can calculate total size`() {
		val testData = listOf(
			"$ cd /",
			"$ ls",
			"dir a",
			"14848514 b.txt",
			"8504156 c.dat",
			"dir d",
			"$ cd a",
			"$ ls",
			"dir e",
			"29116 f",
			"2557 g",
			"62596 h.lst",
			"$ cd e",
			"$ ls",
			"584 i",
			"$ cd ..",
			"$ cd ..",
			"$ cd d",
			"$ ls",
			"4060174 j",
			"8033020 d.log",
			"5626152 d.ext",
			"7214296 k"
		)

		val d = Day7()
		var index = 0
		testData.forEach { putDataOnTopic("${index++}", it, d.inputTopic) }

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals((94853 + 584).toString(), records.last { it.key() == "${day}$part1" }.value())
		assertEquals("24933642", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}
}
