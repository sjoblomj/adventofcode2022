package org.sjoblomj.adventofcode.day2

import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.util.*

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day2.Day2")

const val day = "day2"

fun day2() {
	val d = Day2()
	KafkaPublisher().readFile("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic)
	logger.info("Total score is {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("The three elves carrying the most calories are together carrying {} calories",
		records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day2 {
	val inputTopic = "${day}_${UUID.randomUUID()}"

	internal fun solve(): KafkaStreamsSetup {
		val streamsBuilder = StreamsBuilder()
		readInputStream(streamsBuilder)
		return KafkaStreamsSetup(streamsBuilder)
	}


	private fun readInputStream(streamsBuilder: StreamsBuilder) {
		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
	}
}

