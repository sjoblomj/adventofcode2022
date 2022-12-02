package org.sjoblomj.adventofcode.day1

import org.apache.kafka.common.serialization.Serde
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

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day1.Day1")

const val day = "day1"

fun day1() {
	val d = Day1()
	KafkaPublisher().readFile("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("$day$part1", "$day$part2"))
	logger.info("The elf carrying the most calories are carrying {} calories",
		records.first { it.key() == "$day$part1" }.value())
	logger.info("The three elves carrying the most calories are together carrying {} calories",
		records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day1 {
	val inputTopic = "${day}_${UUID.randomUUID()}"
	private val groupedTopic = "${inputTopic}_grouped_summation"
	private val mutableListSerde: Serde<MutableList<Int>> = MutableListSerde()

	internal fun solve(): KafkaStreamsSetup {
		KafkaPublisher().putDataOnTopic(0, null, groupedTopic) // Topic must exist; put a dummy value on it to create

		val streamsBuilder = StreamsBuilder()
		groupAndSum(streamsBuilder)
		part1(streamsBuilder)
		part2(streamsBuilder)
		return KafkaStreamsSetup(streamsBuilder)
	}

	private fun part1(streamsBuilder: StreamsBuilder) {
		findBiggestAmountOfCalories(streamsBuilder, part1, 1)
	}

	private fun part2(streamsBuilder: StreamsBuilder) {
		findBiggestAmountOfCalories(streamsBuilder, part2, 3)
	}

	/**
	 * Consumes [inputTopic] and groups it, so that each elf has a record with its count and the sum of the calories that
	 * it carries. The result is written to [groupedTopic].
	 */
	private fun groupAndSum(streamsBuilder: StreamsBuilder) {
		val materializedSummation = Materialized.`as`<Int, Int, KeyValueStore<Bytes, ByteArray>>("$inputTopic-summed")
		val markerForNewElf = ""
		var elfNum = 1

		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.filter { _, value ->
				if (value == markerForNewElf) {
					elfNum++; false
				} else true
			}
			.mapValues { value -> value.toInt() }
			.groupBy { _, _ -> elfNum }
			.reduce({ cal1, cal2 -> cal1 + cal2 }, materializedSummation)
			.toStream()
			.map { _, value -> KeyValue(0, value) }
			.to(groupedTopic, Produced.with(intSerde, intSerde))
	}

	/**
	 * Consumes [groupedTopic] and keeps the [numberOfBestElves] that carry the highest amount of calories. The sum of
	 * these records are written to [resultTopic]
	 */
	private fun findBiggestAmountOfCalories(streamsBuilder: StreamsBuilder, part: String, numberOfBestElves: Int) {
		val materializedTopList = Materialized
			.`as`<Int, MutableList<Int>, KeyValueStore<Bytes, ByteArray>>("$inputTopic-toplist_for_${numberOfBestElves}_elves")
			.withValueSerde(mutableListSerde)

		streamsBuilder.stream(groupedTopic, Consumed.with(intSerde, intSerde))
			.groupByKey()
			.aggregate(
				{ mutableListOf() },
				{ _, value, aggregate -> aggregate.addToListIfBigger(value, numberOfBestElves) },
				materializedTopList
			)
			.toStream()
			.flatMap { key, value -> value.map { KeyValue(key, it) } }
			.groupByKey()
			.reduce { res1, res2 -> res1 + res2 }
			.toStream()
			.map { _, value -> KeyValue("$day$part", value.toString()) }
			.peek { _, value -> logger.info("Result $part: - $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}

	private fun MutableList<Int>.addToListIfBigger(value: Int, capacity: Int): MutableList<Int> {
		this.add(value)
		return this
			.sorted()
			.reversed()
			.take(capacity)
			.toMutableList()
	}
}
