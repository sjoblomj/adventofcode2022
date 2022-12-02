package org.sjoblomj.adventofcode.day1

import org.junit.jupiter.api.Test
import org.sjoblomj.adventofcode.kafka.KafkaPublisher
import org.sjoblomj.adventofcode.kafka.getAllRecords
import org.sjoblomj.adventofcode.kafka.part1
import org.sjoblomj.adventofcode.kafka.resulttopic
import kotlin.test.assertEquals

class Day1Tests {

	@Test
	fun `Can calculate calories for elf carrying most`() {
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

		val kafkaPublisher = KafkaPublisher()
		testData.forEach { kafkaPublisher.putDataOnTopic("test", it, inputtopic) }

		val stream = performPart1()

		val expectedResult = (7000 + 8000 + 9000).toString()
		val records = getAllRecords(resulttopic)
		assertEquals(expectedResult, records.first { it.key() == "$day$part1" }.value())
		stream.close()
	}
}
