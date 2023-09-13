package org.sjoblomj.adventofcode.day3

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.*
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day3.Day3")

const val day = "day3"

fun day3() {
	val d = Day3()
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Sum of priorities of items in both compartments is {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("Sum of priorities of each elf-triplet is {}", records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day3 {
	val inputTopic = "${day}_${UUID.randomUUID()}"

	internal fun solve(): KafkaStreamsSetup<Int, Int> {
		val streamsBuilder = StreamsBuilder()
		part1(streamsBuilder)
		part2(streamsBuilder)
		return KafkaStreamsSetup(streamsBuilder.build(), intSerde, intSerde)
	}

	private fun part1(streamsBuilder: StreamsBuilder) {
		val pairedStream = streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.map { key, value -> KeyValue(key, value.substring(0, value.length / 2) to value.substring(value.length / 2)) }

		val  firstCompartment = sendUniqueLettersToTopic(pairedStream.mapValues { (firstCompartment, _)  ->  firstCompartment })
		val secondCompartment = sendUniqueLettersToTopic(pairedStream.mapValues { (_, secondCompartment) -> secondCompartment })

		val commonLettersBetweenCompartments = firstCompartment.joinValues(secondCompartment)

		calculatePrioritySum(commonLettersBetweenCompartments, part1)
	}

	private fun part2(streamsBuilder: StreamsBuilder) {

		fun BranchedKStream<String, String>.branchOnKeyIndex(index: String, branchName: String) =
			this.branch({ key, _ -> key.split("_")[1] == index }, Branched.withFunction({ ks -> ks }, branchName))

		// Branch the steam, so that each elf is on its own branch.
		val elfIndexBranchedStream = streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.map { key, value -> KeyValue("${key.toInt() / 3}_${key.toInt() % 3}", value) }
			.split(Named.`as`("elftriplets-"))
			.branchOnKeyIndex("0", "first")
			.branchOnKeyIndex("1", "second")
			.branchOnKeyIndex("2", "third")
			.noDefaultBranch()

		fun sendEachLetterOfBranchToTopic(branchName: String) = sendUniqueLettersToTopic(
			elfIndexBranchedStream[branchName]!!
				.map { key, value -> KeyValue(key.split("_")[0], value) }
		)

		val firstElfLetters  = sendEachLetterOfBranchToTopic("elftriplets-first")
		val secondElfLetters = sendEachLetterOfBranchToTopic("elftriplets-second")
		val thirdElfLetters  = sendEachLetterOfBranchToTopic("elftriplets-third")

		val commonLettersBetweenElves = firstElfLetters.joinValues(secondElfLetters).joinValues(thirdElfLetters)

		calculatePrioritySum(commonLettersBetweenElves, part2)
	}

	private fun sendUniqueLettersToTopic(stream: KStream<String, String>) = stream
		.mapValues { letters -> letters.toCharArray().distinct() }
		.flatMap { key, characters -> characters.map { char -> KeyValue("${key}_${char}", char.toString()) } }

	private fun calculatePrioritySum(stream: KStream<String, String>, part: String) {
		val branchPrefix = "${day}_$part-lowercaseUppercase-"

		val uppercaseLowercaseBranches = stream
			.split(Named.`as`(branchPrefix))
			.branch({ _, letter -> letter.first().isLowerCase() }, Branched.withFunction({ ks -> ks }, "lowercase"))
			.branch({ _, letter -> letter.first().isUpperCase() }, Branched.withFunction({ ks -> ks }, "uppercase"))
			.noDefaultBranch()

		uppercaseLowercaseBranches["${branchPrefix}lowercase"]!!.mapValues { letter -> letter.first() - 'a' + 1 }
			.merge(uppercaseLowercaseBranches["${branchPrefix}uppercase"]!!.mapValues { letter -> letter.first() - 'A' + 27 })
			.map { _, value -> KeyValue(0, value) }
			.groupByKey()
			.reduce { res1, res2 -> res1 + res2 }
			.toStream()
			.map { _, value -> KeyValue("$day$part", value.toString()) }
			.peek { _, value -> logger.info("Result $part: $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}

	private fun KStream<String, String>.joinValues(other: KStream<String, String>) =
		this.join(
			other,
			{ _, otherValue -> otherValue },
			JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(1)),
			StreamJoined.with(stringSerde, stringSerde, stringSerde)
		)
}
