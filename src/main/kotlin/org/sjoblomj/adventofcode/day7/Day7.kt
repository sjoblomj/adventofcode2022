package org.sjoblomj.adventofcode.day7

import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.util.*

const val day = "day7"

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.${day}.${day.replaceFirstChar { it.uppercaseChar() }}")

fun day7() {
	val d = Day7()
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Sum of dirs with size less than ${d.maxDirSize}: {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("Start-of-message marker comes after position {}", records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day7 {
	val inputTopic = "${day}_${UUID.randomUUID()}"
	val maxDirSize = 100000
	private val storeName = "${inputTopic}_store"

	internal fun solve(): KafkaStreamsSetup<String, Long> {
		val streamsBuilder = StreamsBuilder()
		listOf("${storeName}_$part1").forEach { name ->
			streamsBuilder.addStateStore(KeyValueStoreBuilder(inMemoryKeyValueStore(name), stringSerde, MutableListSerde(), Time.SYSTEM))
		}

		solve(streamsBuilder, part1)

		return KafkaStreamsSetup(streamsBuilder.build(), stringSerde, longSerde)
	}

	private fun solve(streamsBuilder: StreamsBuilder, part: String) {
		val storeName = "${storeName}_$part"

		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.process(ProcessorSupplier { Parser(storeName) }, storeName)
			.groupByKey()
			.reduce { size1, size2 -> size1 + size2 }
			.filter { _, value -> value < maxDirSize }
			.groupBy { _, value -> KeyValue("$day$part", value) }
			.reduce({ size1, size2 -> size1 + size2 }, { size1, size2 -> size1 - size2 })
			.toStream()
			.map { _, value -> KeyValue("$day$part", value.toString()) }
			.peek { _, value -> logger.info("Result $part: $value") }
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
				forwardFileSizeToAllParentDirs(record, command[0].toLong(), command[1])
			}
		}

		override fun close() {
		}


		private fun parseDirectoryChange(dirName: String) {
			val dirs = if (dirName == "..") {
				store[pwd].dropLast(1).toMutableList()
			} else {
				store[pwd].also { it.add(dirName) }
			}
			store.put(pwd, dirs)
		}


		private fun forwardFileSizeToAllParentDirs(record: Record<String, String>, fileSize: Long, fileName: String) {
			(1 .. store[pwd].size).forEach { numberOfDirs ->
				val dirs = store[pwd].take(numberOfDirs)
				val path = dirs.joinToString("/").replace("^//".toRegex(), "/")

				val pathWithFileName = store[pwd].plus(fileName).joinToString("/").replace("^//".toRegex(), "/")
				val r = Record(path, fileSize, record.timestamp())
				context.forward(r)
			}
		}

		private fun String.isNumeric() = this.toLongOrNull() != null
	}
}
