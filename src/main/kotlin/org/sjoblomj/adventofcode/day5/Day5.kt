package org.sjoblomj.adventofcode.day5

import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.processor.PunctuationType
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import org.apache.kafka.streams.processor.api.Record
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day5.Day5")

const val day = "day5"

fun day5() {
	val d = Day5()
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Part1: {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("Part2: {}", records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day5 {
	val inputTopic = "${day}_${UUID.randomUUID()}"

	internal fun solve(): KafkaStreamsSetup<String, String> {
		val topology = part1()
		return KafkaStreamsSetup(topology, stringSerde, stringSerde)
	}

	private fun part1(): Topology {
		val source = "${inputTopic}_source"
		val sink = "${inputTopic}_sink"
		val parserName = "${inputTopic}_parser"
		val stackAccumulatorName = "${inputTopic}_stackAccumulatorBuilder"
		val stackBuilderName = "${inputTopic}_stackbuilder"
		val movementProcessorName = "${inputTopic}_movementprocessor"
		val stackAccumulatorStoreName = "${inputTopic}_stackAccumulatorStore"
		val stackStoreName = "${inputTopic}_stackStore"

		val stackAccumulatorStore = Stores.inMemoryKeyValueStore(stackAccumulatorStoreName)
		val stackStore = Stores.inMemoryKeyValueStore(stackStoreName)
		val stackAccumulatorStoreBuilder = KeyValueStoreBuilder(stackAccumulatorStore, stringSerde, MutableListSerde(), Time.SYSTEM)
		val stackStoreBuilder = KeyValueStoreBuilder(stackStore, stringSerde, MutableListSerde(), Time.SYSTEM)

		return Topology()
			.addSource(source, inputTopic)
			.addProcessor(parserName, ProcessorSupplier { Parser(stackAccumulatorStoreName, stackAccumulatorName, stackBuilderName, movementProcessorName) }, source)
			.addProcessor(stackAccumulatorName, ProcessorSupplier { StackAccumulator(stackAccumulatorStoreName) }, parserName)
			.addProcessor(stackBuilderName, ProcessorSupplier { StackBuilder(stackStoreName) }, parserName)
			.addProcessor(movementProcessorName, ProcessorSupplier { MovementProcessor(stackStoreName, 3) }, parserName)
			.addStateStore(stackAccumulatorStoreBuilder, parserName)
			.addStateStore(stackAccumulatorStoreBuilder, stackAccumulatorName)
			.addStateStore(stackStoreBuilder, stackBuilderName)
			.addStateStore(stackStoreBuilder, movementProcessorName)
			.addSink(sink, resultTopic, movementProcessorName)
	}


	private class Parser(
		private val stateStoreName: String,
		private val stackAccumulatorBuilderName: String,
		private val stackBuilderName: String,
		private val movementProcessorName: String
	) : Processor<String, String, String, String> {

		private lateinit var context: ProcessorContext<String, String>

		private val stackRegex = "([\\[ ][A-Z ][] ] ?)+".toRegex()
		private val moveRegex = "move (?<num>\\d+) from (?<source>\\d+) to (?<dest>\\d+)".toRegex()


		override fun init(context: ProcessorContext<String, String>) {
			this.context = context
		}

		override fun process(record: Record<String, String>) {
			logger.info("ParserProcessor - line: ${record.key()}, value: '${record.value()}'")

			if (stackRegex.matches(record.value())) {
				sendStackBuildInfoToProcessor(record)

			} else if (moveRegex.matches(record.value())) {
				sendMovementInfoToProcessor(record)

			} else if (record.value() == "") {
				reverseStackAccumulation(record)
			}
		}

		private fun sendStackBuildInfoToProcessor(record: Record<String, String>) {

			fun String.replaceAll(old: String, new: String): String {
				val newStr = this.replace(old, new)
				return if (newStr == this)
					newStr
				else
					newStr.replaceAll(old, new)
			}


			val stacks = record.value()
				.replace(" {3}$".toRegex(), "[ ]")
				.replace("^ {3}".toRegex(), "[ ]")
				.replaceAll("     ", " [ ] ")
				.replace(" ?\\[".toRegex(), ",")
				.replace("] ?".toRegex(), "")
				.drop(1)
			logger.info("Parsed rows from '${record.value()}' to '${stacks}'")

			val r = Record(record.key(), stacks, record.timestamp())
			context.forward(r, stackAccumulatorBuilderName)
		}

		private fun sendMovementInfoToProcessor(record: Record<String, String>) {
			val regexGroups = moveRegex.matchEntire(record.value())!!.groups
			val num = regexGroups["num"]?.value
			val source = regexGroups["source"]?.value?.toInt()?.minus(1)
			val dest = regexGroups["dest"]?.value?.toInt()?.minus(1)

			if (num == null || source == null || dest == null)
				throw RuntimeException("Failed to parse ${record.value()}")

			val movement = "$source to $dest"
			val r = Record(num, movement, record.timestamp())
			context.forward(r, movementProcessorName)
		}

		private fun reverseStackAccumulation(record: Record<String, String>) {
			val store: KeyValueStore<String, MutableList<String>> = context.getStateStore(stateStoreName)
			store.reverseAll().use {
				while (it.hasNext()) {

					val kv = it.next()
					logger.info("Reading rows in reverse - row: ${kv.key}, items: ${kv.value}")

					kv.value.forEachIndexed { index, item ->
						if (item != "") {
							val rec = Record(index.toString(), item, record.timestamp())
							context.forward(rec, stackBuilderName)
						}
					}
				}
			}
		}

		override fun close() {
		}
	}


	private class MovementProcessor(
		private val stateStoreName: String,
		private val punctuationInterval: Long
	) : Processor<String, String, String, String> {

		private lateinit var context: ProcessorContext<String, String>
		private lateinit var store: KeyValueStore<String, MutableList<String>>

		override fun init(context: ProcessorContext<String, String>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)

			context.schedule(Duration.ofSeconds(punctuationInterval), PunctuationType.WALL_CLOCK_TIME, this::forwardTopItem)
		}

		override fun process(record: Record<String, String>) {

			val moveRegex = "(?<source>\\d+) to (?<dest>\\d+)".toRegex()
			val regexGroups = moveRegex.matchEntire(record.value())!!.groups
			val source = regexGroups["source"]!!.value
			val dest = regexGroups["dest"]!!.value
			val itemsToMove = record.key().toInt()

			logger.info("MovementProcessor $itemsToMove times from stack $source to stack $dest")

			move(part1, source, dest) { sourceName: String, destName: String, part: String ->
				moveItemNTimes(itemsToMove, sourceName, destName, part)
			}

			move(part2, source, dest) { sourceName: String, destName: String, part: String ->
				moveNItems(itemsToMove, sourceName, destName, part)
			}
		}

		private fun move(part: String, source: String, dest: String, moveFunction: (String, String, String) -> Unit) {
			val sourceName = "${part}_$source"
			val destName = "${part}_$dest"
			logger.info("$part: MovementProcessor stack before from: ${store[sourceName]}")
			logger.info("$part: MovementProcessor stack before to:   ${store[destName]}")

			moveFunction.invoke(sourceName, destName, part)

			logger.info("$part: MovementProcessor stack after  from: ${store[sourceName]}")
			logger.info("$part: MovementProcessor stack after  to:   ${store[destName]}")
		}

		override fun close() {
		}


		private fun moveItemNTimes(itemsToMove: Int, sourceName: String, destName: String, part: String) {
			repeat((0 until itemsToMove).count()) {
				moveNItems(1, sourceName, destName, part)
			}
		}

		private fun moveNItems(itemsToMove: Int, sourceName: String, destName: String, part: String) {
			val item = removeFromStack(sourceName, itemsToMove)
			addToStack(destName, item)
			logger.info("$part: MovementProcessor items: $item")
		}

		private fun removeFromStack(stackName: String, itemsToRemove: Int): List<String> {
			val item = store[stackName].takeLast(itemsToRemove)
			store.put(stackName, store[stackName].dropLast(itemsToRemove).toMutableList())
			return item
		}

		private fun addToStack(stackName: String, items: List<String>) {
			val stack = store[stackName]
			stack.addAll(items)
			store.put(stackName, stack)
		}

		private fun forwardTopItem(timestamp: Long) {
			forwardTopItemsForPart(timestamp, part1)
			forwardTopItemsForPart(timestamp, part2)
		}

		private fun forwardTopItemsForPart(timestamp: Long, part: String) {
			var letters = ""

			store.all().use {
				while (it.hasNext()) {
					val kv = it.next()
					if (kv.key.contains(part)) {
						letters += kv.value.last()
					}
				}
			}

			logger.info("$part: Puctuation - sending '$letters'")
			val rec = Record("$day$part", letters, timestamp)
			context.forward(rec)
		}
	}


	private class StackAccumulator(private val stateStoreName: String) : Processor<String, String, String, String> {
		private lateinit var context: ProcessorContext<String, String>
		private lateinit var store: KeyValueStore<String, MutableList<String>>

		override fun init(context: ProcessorContext<String, String>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)
		}

		override fun process(record: Record<String, String>) {

			logger.info("StackAccumulatorProcessor - row: ${record.key()}, item: '${record.value()}'")

			record.value().split(",").forEach { addToStack(record.key(), it) }
		}

		override fun close() {
		}


		private fun addToStack(rowName: String, item: String) {
			logger.info("Adding '$item' to row $rowName")
			val stack = store[rowName] ?: mutableListOf()
			stack.add(item)
			store.put(rowName, stack)
		}
	}


	private class StackBuilder(private val stateStoreName: String) : Processor<String, String, String, String> {
		private lateinit var context: ProcessorContext<String, String>
		private lateinit var store: KeyValueStore<String, MutableList<String>>

		override fun init(context: ProcessorContext<String, String>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)
		}

		override fun process(record: Record<String, String>) {
			if (record.value().isBlank())
				logger.info("StackBuilderProcessor - stack: ${record.key()}, item: '${record.value()}' - Ignoring")
			else {
				logger.info("StackBuilderProcessor - stack: ${record.key()}, item: '${record.value()}'")
				addToStack(record.key(), record.value(), part1)
				addToStack(record.key(), record.value(), part2)
			}
		}

		override fun close() {
		}


		private fun addToStack(stackName: String, item: String, prefix: String) {
			logger.info("$prefix: Adding '$item' to stack $stackName")
			val stack = store["${prefix}_$stackName"] ?: mutableListOf()
			stack.add(item)
			store.put("${prefix}_$stackName", stack)
		}
	}
}
