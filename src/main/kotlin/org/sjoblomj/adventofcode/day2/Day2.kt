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
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Total score when interpreting as moves is {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("Total score when interpreting as desired results is {}", records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day2 {
	val inputTopic = "${day}_${UUID.randomUUID()}"
	private val totalScorePart1Topic = "${inputTopic}_total_score_$part1"
	private val totalScorePart2Topic = "${inputTopic}_total_score_$part2"

	internal fun solve(): KafkaStreamsSetup {
		putDataOnTopic(null, null, totalScorePart1Topic) // Topic must exist; put a dummy value on it to create
		putDataOnTopic(null, null, totalScorePart2Topic) // Topic must exist; put a dummy value on it to create

		val streamsBuilder = StreamsBuilder()
		part1(streamsBuilder)
		part2(streamsBuilder)
		calculateTotalScore(streamsBuilder, part1, totalScorePart1Topic)
		calculateTotalScore(streamsBuilder, part2, totalScorePart2Topic)
		return KafkaStreamsSetup(streamsBuilder.build())
	}

	private fun part1(streamsBuilder: StreamsBuilder) {
		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.to(totalScorePart1Topic, Produced.with(stringSerde, stringSerde))
	}

	private fun part2(streamsBuilder: StreamsBuilder) {
		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.map { _, value -> KeyValue(0, value.split(" ").first() to value.split(" ").last()) }
			.mapValues { (opponent, result) -> opponent to calculateYourMove(result, opponent) }
			.map { _, (opponent, you) -> KeyValue("", "$opponent $you") }
			.to(totalScorePart2Topic, Produced.with(stringSerde, stringSerde))
	}

	private fun calculateTotalScore(streamsBuilder: StreamsBuilder, part: String, totalScoreTopic: String) {

		streamsBuilder.stream(totalScoreTopic, Consumed.with(stringSerde, stringSerde))
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

private fun calculateYourMove(result: String, opponent: String) = when (result) {
	youShouldWin -> {
		when (opponent) {
			rockO -> paperY
			paperO -> scissorsY
			else -> rockY
		}
	}
	youShouldLose -> {
		when (opponent) {
			rockO -> scissorsY
			paperO -> rockY
			else -> paperY
		}
	}
	else -> {
		when (opponent) {
			rockO -> rockY
			paperO -> paperY
			else -> scissorsY
		}
	}
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
const val youShouldLose = "X"
const val youShouldWin = "Z"
val symbolMap = hashMapOf(
	rockO to rockPoints,
	paperO to paperPoints,
	scissorsO to scissorsPoints,
	rockY to rockPoints,
	paperY to paperPoints,
	scissorsY to scissorsPoints
)
