package org.sjoblomj.adventofcode.day4

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day4.Day4")

const val day = "day4"

fun day4() {
	val d = Day4()
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	Thread.sleep(10_000)
	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Number of assignment pairs with one range fully contained is {}", records.last { it.key() == "$day$part1" }.value())
	logger.info("Number of assignment pairs with range overlaps {}", records.last { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day4 {
	val inputTopic = "${day}_${UUID.randomUUID()}"

	internal fun solve(): KafkaStreamsSetup<Int, Int> {
		val streamsBuilder = StreamsBuilder()

		val pairedStream = streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.map { key, value -> KeyValue(key, value.split(",")[0] to value.split(",")[1]) }
		val sectionsInFirstRange  = expandListAndSendToTopic(pairedStream.mapValues { (fst, _) -> fst })
		val sectionsInSecondRange = expandListAndSendToTopic(pairedStream.mapValues { (_, snd) -> snd })

		part1(sectionsInFirstRange, sectionsInSecondRange)
		part2(sectionsInFirstRange, sectionsInSecondRange)
		return KafkaStreamsSetup(streamsBuilder.build(), intSerde, intSerde)
	}

	private fun part1(sectionsInFirstRange: KStream<String, Int>, sectionsInSecondRange: KStream<String, Int>) {

		val commonBetweenPairs = sectionsInFirstRange.joinValues(sectionsInSecondRange)
		val countOfOverlaps    = countEventsOnTopic(commonBetweenPairs)
		val countInFirstRange  = countEventsOnTopic(sectionsInFirstRange)
		val countInSecondRange = countEventsOnTopic(sectionsInSecondRange)

		val counts = countOfOverlaps.joinValues(countInFirstRange).merge(countOfOverlaps.joinValues(countInSecondRange))
			.groupByKey(Grouped.keySerde(stringSerde)) // Group by key to remove duplicates from both first and second range
			.count()
			.toStream()
			.map { _, value -> KeyValue("0_0", value.toInt()) }

		countEventsOnTopic(counts)
			.map { _, value -> KeyValue("$day$part1", value.toString()) }
			.peek { _, value -> logger.info("Result $part1: $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}

	private fun part2(sectionsInFirstRange: KStream<String, Int>, sectionsInSecondRange: KStream<String, Int>) {

		countEventsOnTopic(sectionsInFirstRange.joinValues(sectionsInSecondRange))
			.map { key, value -> KeyValue(key.split("_")[0].toInt(), value) }
			.groupByKey()
			.count()
			.toStream()
			.filter { _, value -> value == 1L } // Late events seem to add duplicates - filter to only keep the first
			.map { _, value -> KeyValue(0, value.toInt()) }
			.groupByKey()
			.count()
			.toStream()
			.map { _, value -> KeyValue("$day$part2", value.toString()) }
			.peek { _, value -> logger.info("Result $part2: $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}


	private fun countEventsOnTopic(stream: KStream<String, Int>) =
		stream
			.map { key, value -> KeyValue(key.split("_")[0].toInt(), value) }
			.groupByKey()
			.count()
			.toStream()
			.filter { key, value -> key != null && value != null }
			.map { key, value -> KeyValue("${key}_${value}", value.toInt()) }

	private fun expandListAndSendToTopic(stream: KStream<String, String>) = stream
		.mapValues { rangeString -> rangeString.split("-")[0].toInt() to rangeString.split("-")[1].toInt() }
		.mapValues { (lowBound, highBound) -> (lowBound..highBound).toList() }
		.flatMap { key, sectionList -> sectionList.map { section -> KeyValue("${key}_${section}", section) } }

	private fun KStream<String, Int>.joinValues(other: KStream<String, Int>) =
		this.join(
			other,
			{ _, otherValue -> otherValue },
			JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
			StreamJoined.with(stringSerde, intSerde, intSerde)
		)
}
