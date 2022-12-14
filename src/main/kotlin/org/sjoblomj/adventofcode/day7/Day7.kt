package org.sjoblomj.adventofcode.day7

import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*
import kotlin.streams.toList

const val day = "day7"

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.${day}.${day.replaceFirstChar { it.uppercaseChar() }}")

fun day7() {
	val d = Day7()
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Sum of dirs with size less than ${d.maxDirSize}: {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("Size of directory to delete: {}", records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day7 {
	val inputTopic = "${day}_${UUID.randomUUID()}"
	val maxDirSize = 100000
	private val totalDiskSpace = 70000000L
	private val unusedSpaceNeeded = 30000000L
	private val listStoreName = "${inputTopic}_store_list"
	private val sizeStoreName = "${inputTopic}_store_size"

	internal fun solve(): KafkaStreamsSetup<String, Long> {
		val streamsBuilder = StreamsBuilder()

		listOf(
			"${listStoreName}_$part1" to MutableListSerde(),
			"${listStoreName}_$part2" to MutableListSerde(),
			sizeStoreName to longSerde
		).forEach { (name, serde) ->
			streamsBuilder.addStateStore(KeyValueStoreBuilder(inMemoryKeyValueStore(name), stringSerde, serde, Time.SYSTEM))
		}

		part1(streamsBuilder)
		part2(streamsBuilder)

		return KafkaStreamsSetup(streamsBuilder.build(), stringSerde, longSerde)
	}

	private fun part1(streamsBuilder: StreamsBuilder) {
		val storeName = "${listStoreName}_$part1"

		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.process(ProcessorSupplier { Parser(storeName) }, storeName)
			.groupByKey()
			.reduce { size1, size2 -> size1 + size2 }
			.filter { _, value -> value < maxDirSize }
			.groupBy { _, value -> KeyValue("", value) }
			.reduce({ size1, size2 -> size1 + size2 }, { size1, size2 -> size1 - size2 })
			.toStream()
			.map { _, value -> KeyValue("$day$part1", value.toString()) }
			.peek { _, value -> logger.info("Result $part1: $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}

	private fun part2(streamsBuilder: StreamsBuilder) {
		val listStoreName = "${listStoreName}_$part2"

		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.process(ProcessorSupplier { Parser(listStoreName) }, listStoreName)
			.groupByKey()
			.reduce { size1, size2 -> size1 + size2 }
			.toStream()
			.process(ProcessorSupplier { DirectoryToDeleteProcessor(sizeStoreName, totalDiskSpace, unusedSpaceNeeded) }, sizeStoreName)
			.map { _, value -> KeyValue("$day$part2", value.toString()) }
			.peek { _, value -> logger.info("Result $part2: $value") }
			.to(resultTopic, Produced.with(stringSerde, stringSerde))
	}


	private class Parser(private val stateStoreName: String) : Processor<String, String, String, Long> {
		private lateinit var context: ProcessorContext<String, Long>
		private lateinit var store: KeyValueStore<String, MutableList<String>>
		private val pwd = "pwd"

		override fun init(context: ProcessorContext<String, Long>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)

			store.put(pwd, mutableListOf())
		}

		override fun process(record: Record<String, String>) {
			val command = record.value().split(" ")

			if (command[0] == "$" && command[1] == "cd") {
				parseDirectoryChange(command[2])
			} else if (command[0].isNumeric()) {
				forwardFileSizeToAllParentDirs(command[0].toLong(), record.timestamp())
			}
		}

		override fun close() {
		}


		/**
		 * Adds the given [dirName] to the state store that keeps track of the directory of the current file.
		 * If [dirName] is "..", the last directory is dropped from the state store.
		 */
		private fun parseDirectoryChange(dirName: String) {
			val dirs = if (dirName == "..") {
				store[pwd].dropLast(1).toMutableList()
			} else {
				store[pwd].also { it.add(dirName) }
			}
			store.put(pwd, dirs)
		}


		/**
		 * Given a [fileSize] that will be used as the record value, this will produce one record *for each*
		 * parent-directory of the file (where the directories of the file is given by the state store). All produced
		 * records will have the given [timestamp].
		 *
		 * For example, if [fileSize] is 71, and the pwd of the state store is [/, apa, bepa, cepa] (indicating that the
		 * file is in directory /apa/bepa/cepa), then records with these KeyValues will be created:
		 * {"/", 71},
		 * {"/apa", 71},
		 * {"/apa/bepa", 71},
		 * {"/apa/bepa/cepa", 71}
		 */
		private fun forwardFileSizeToAllParentDirs(fileSize: Long, timestamp: Long) {
			(1 .. store[pwd].size).forEach { numberOfDirs ->
				val dirs = store[pwd].take(numberOfDirs)
				val path = dirs.joinToString("/").replace("^//".toRegex(), "/")

				context.forward(Record(path, fileSize, timestamp))
			}
		}

		private fun String.isNumeric() = this.toLongOrNull() != null
	}


	private class DirectoryToDeleteProcessor(
		private val stateStoreName: String,
		private val totalDiskSpace: Long,
		private val unusedSpaceNeeded: Long
	) : Processor<String, Long, String, Long> {
		private lateinit var context: ProcessorContext<String, Long>
		private lateinit var store: KeyValueStore<String, Long>
		private var spaceToClear: Long? = null

		override fun init(context: ProcessorContext<String, Long>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)

			context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, this::forwardSizeOfDirToClear)
		}

		override fun process(record: Record<String, Long>) {
			if (record.key() == "/") {
				spaceToClear = unusedSpaceNeeded - (totalDiskSpace - record.value())
				logger.info("root has size ${record.value()}, need to clear $spaceToClear")
			}
			store.put(record.key(), record.value())
		}

		override fun close() {
		}


		private fun forwardSizeOfDirToClear(timestamp: Long) {
			val amountOfSpaceToClear = spaceToClear ?: return

			val sizes = mutableListOf<Pair<String, Long>>()
			store.all().use {
				while (it.hasNext()) {
					val kv = it.next()
					sizes.add(kv.key to kv.value)
				}
			}

			val sizeOfDirToClear = sizes
				.sortedBy { it.second }
				.stream()
				.peek { println("${it.first} : ${it.second} ${if (it.second < amountOfSpaceToClear) "<" else ">"} $amountOfSpaceToClear") }
				.toList()
				.filter { it.second > amountOfSpaceToClear }
				.minOf { it.second }

			context.forward(Record("", sizeOfDirToClear, timestamp))
		}
	}
}
