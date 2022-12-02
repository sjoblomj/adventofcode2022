package org.sjoblomj.adventofcode.day1

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.state.KeyValueStore
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.math.max

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day1.Day1")

const val day = "day1"

fun day1() {
	val day1 = Day1()
	KafkaPublisher().readFile("src/main/resources/inputs/$day.txt", day1.inputtopic)
	val stream = day1.performPart1()

	val records = getAllRecords(resulttopic)
	logger.info("The elf carrying the most calories are carrying {} calories",
		records.first { it.key() == "$day$part1" }.value())
	logger.info("The three elves carrying the most calories are together carrying {} calories",
		records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day1 {
	val inputtopic = "${day}_${UUID.randomUUID()}"

	internal fun performPart1(): KafkaStreamsSetup {
		val streamsBuilder = StreamsBuilder()
		day1part1(streamsBuilder)
		return KafkaStreamsSetup(streamsBuilder)
	}


	private fun day1part1(streamsBuilder: StreamsBuilder) {
		val materialized = Materialized.`as`<Int, Int, KeyValueStore<Bytes, ByteArray>>("$inputtopic-summed")
		val markerForNewElf = ""
		var elfNum = 0

		streamsBuilder.stream(inputtopic, Consumed.with(stringSerde, stringSerde))
			.filter { _, value -> if (value == markerForNewElf) { elfNum++; false } else true }
			.mapValues { value -> value.toInt() }
			.groupBy { _, _ -> elfNum }
			.reduce({ cal1, cal2 -> cal1 + cal2 }, materialized)
			.toStream()
			.map { _, value -> KeyValue(0, value) }
			.groupByKey()
			.reduce { res1, res2 -> max(res1, res2) }
			.toStream()
			.map { _, value -> KeyValue("$day$part1", value.toString()) }
			.peek { _, value -> logger.info("Result: - $value") }
			.to(resulttopic, Produced.with(stringSerde, stringSerde))
	}
}
