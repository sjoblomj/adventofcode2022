package org.sjoblomj.adventofcode.day2

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.*
import kotlin.test.assertEquals

class Day2Tests {

	@Test
	fun `Calculates the correct answers`() {
		val d = Day2()
		KafkaPublisher().readFile("src/main/resources/inputs/${day}.txt", d.inputTopic)

		val stream = d.solve()

		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals("10941", records.last { it.key() == "${day}$part1" }.value())
		assertEquals("13071", records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}

	@Test
	fun `Can calculate total score`() {
		val testData = listOf(
			"A Y",
			"B X",
			"C Z"
		)

		val d = Day2()
		val kafkaPublisher = KafkaPublisher()
		testData.forEach { kafkaPublisher.putDataOnTopic("test", it, d.inputTopic) }

		val stream = d.solve()

		val expectedResultPart1 = ((paperPoints + winPoints) + (rockPoints + losePoints) + (scissorsPoints + drawPoints)).toString()
		val expectedResultPart2 = ((rockPoints + drawPoints) + (rockPoints + losePoints) + (rockPoints + winPoints)).toString()
		val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
		assertEquals(expectedResultPart1, records.last { it.key() == "${day}$part1" }.value())
		assertEquals(expectedResultPart2, records.last { it.key() == "${day}$part2" }.value())
		stream.close()
	}
}
