package org.sjoblomj.adventofcode.day2

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
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

	stream.close()
}

class Day2 {
	val inputTopic = "${day}_${UUID.randomUUID()}"

	internal fun solve(): KafkaStreamsSetup {
		val streamsBuilder = StreamsBuilder()
		calculateTotalScore(streamsBuilder, part1)
		return KafkaStreamsSetup(streamsBuilder)
	}

	private fun calculateTotalScore(streamsBuilder: StreamsBuilder, part: String) {

		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.filter { _, value -> value != null }
			.map { _, value -> KeyValue(0, value.split(" ").first() to value.split(" ").last()) }
			.mapValues { (opponent, you) -> symbolMap[you]!! +
				if (opponentWins(opponent, you)) losePoints
				else if (opponentLoses(opponent, you)) winPoints
				else drawPoints
			}
			.groupByKey()
			.reduce { v1, v2 -> v1 + v2 }
			.toStream()
			.map { _, value -> KeyValue("$day$part", value.toString()) }
			.peek { _, value -> logger.info("Result $part: $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}
}

private fun opponentWins(opponent: String, you: String): Boolean {
	return (
		opponent == rockO && you == scissorsY ||
		opponent == paperO && you == rockY ||
		opponent == scissorsO && you == paperY
	)
}

private fun opponentLoses(opponent: String, you: String): Boolean {
	return (
		opponent == rockO && you == paperY ||
		opponent == paperO && you == scissorsY ||
		opponent == scissorsO && you == rockY
	)
}

const val losePoints = 0
const val drawPoints = 3
const val winPoints = 6
const val rockPoints = 1
const val paperPoints = 2
const val scissorsPoints = 3
const val rockO = "A"
const val rockY = "X"
const val paperO = "B"
const val paperY = "Y"
const val scissorsO = "C"
const val scissorsY = "Z"
val symbolMap = hashMapOf(
	rockO to rockPoints,
	paperO to paperPoints,
	scissorsO to scissorsPoints,
	rockY to rockPoints,
	paperY to paperPoints,
	scissorsY to scissorsPoints
)
