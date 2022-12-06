package org.sjoblomj.adventofcode.day1

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day1Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day1()
		KafkaPublisher().readFile("src/main/resources/inputs/$day.txt", d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("$day$part1", "$day$part2"))
		assertEquals("71300", records.last { it.key() == "$day$part1" }.value())
		assertEquals("209691", records.last { it.key() == "$day$part2" }.value())
		stream.close()
	}

	@Test
	fun `Can calculate calories for the elves carrying the most`() {
		val testData = listOf(
			"1000",
			"2000",
			"3000",
			"",
			"4000",
			"",
			"5000",
			"6000",
			"",
			"7000",
			"8000",
			"9000",
			"",
			"10000"
		)

		val d = Day1()
		val kafkaPublisher = KafkaPublisher()
		testData.forEach { kafkaPublisher.putDataOnTopic("test", it, d.inputTopic) }

		val stream = d.solve()

		val expectedResultPart1 = (7000 + 8000 + 9000).toString()
		val expectedResultPart2 = ((7000 + 8000 + 9000) + (5000 + 6000) + (10000)).toString()
		val records = getAllRecords(resultTopic, listOf("$day$part1", "$day$part2"))
		assertEquals(expectedResultPart1, records.last { it.key() == "$day$part1" }.value())
		assertEquals(expectedResultPart2, records.last { it.key() == "$day$part2" }.value())
		stream.close()
	}
}
