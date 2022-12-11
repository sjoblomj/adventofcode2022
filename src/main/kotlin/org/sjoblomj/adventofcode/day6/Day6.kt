package org.sjoblomj.adventofcode.day6

import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.utils.Time
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.SlidingWindows
import org.apache.kafka.streams.processor.api.*
import org.apache.kafka.streams.state.KeyValueStore
import org.apache.kafka.streams.state.Stores.inMemoryKeyValueStore
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder
import org.sjoblomj.adventofcode.kafka.*
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.day6.Day6")

const val day = "day6"

fun day6() {
	val d = Day6()
	readFileToTopic("src/main/resources/inputs/$day.txt", d.inputTopic)
	val stream = d.solve()

	val records = getAllRecords(resultTopic, listOf("${day}$part1", "${day}$part2"))
	logger.info("Start-of-packet  marker comes after position {}", records.first { it.key() == "$day$part1" }.value())
	logger.info("Start-of-message marker comes after position {}", records.first { it.key() == "$day$part2" }.value())

	stream.close()
}

class Day6 {
	val inputTopic = "${day}_${UUID.randomUUID()}"
	private val storeName = "${inputTopic}_store"
	private val numberOfCharactersPart1 = 4
	private val numberOfCharactersPart2 = 14

	internal fun solve(): KafkaStreamsSetup<String, String> {
		val streamsBuilder = StreamsBuilder()
		listOf("${storeName}_$part1", "${storeName}_$part2").forEach { name ->
			streamsBuilder.addStateStore(KeyValueStoreBuilder(inMemoryKeyValueStore(name), stringSerde, intSerde, Time.SYSTEM))
		}

		solve(streamsBuilder, numberOfCharactersPart1, part1)
		solve(streamsBuilder, numberOfCharactersPart2, part2)

		return KafkaStreamsSetup(streamsBuilder.build(), stringSerde, stringSerde)
	}

	private fun solve(streamsBuilder: StreamsBuilder, numberOfCharacters: Int, part: String) {
		val store = "${storeName}_$part"

		streamsBuilder.stream(inputTopic, Consumed.with(stringSerde, stringSerde))
			.mapValues { string -> string.toCharArray() }
			.flatMap { _, characters -> characters.mapIndexed { index, char -> KeyValue(index, char.toString()) } }
			.processValues({ TimestampFaker() })
			.groupBy { _, _ -> "" }
			.windowedBy(SlidingWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(numberOfCharacters.toLong())))
			.reduce { char1, char2 -> char1 + char2 }
			.toStream()
			.filter { _, value -> value.length == numberOfCharacters && value.toCharArray().distinct().size == numberOfCharacters }
			.process(ProcessorSupplier { HeaderToKeyExtractor(store) }, store)
			.processValues({ MinValueFilter(store) }, store)
			.map { key, _ -> KeyValue("$day$part", "${key.toInt() + 1}") }
			.peek { _, value -> logger.info("Result $part: $value") }
			.to(resultTopic)
	}


	/**
	 * This will interpret the record key as the timestamp of the record (epoch). Thus, this Processor will
	 * change the timestamps on the record to be that of the key, multiplied by 1000 (to convert to seconds).
	 */
	private class TimestampFaker<T> : FixedKeyProcessor<Int, T, T> {
		private lateinit var context: FixedKeyProcessorContext<Int, T>
		override fun init(context: FixedKeyProcessorContext<Int, T>) {
			this.context = context
		}

		override fun process(record: FixedKeyRecord<Int, T>) {
			record.headers().add(RecordHeader("index", record.key().toString().toByteArray()))
			context.forward(record.withTimestamp((record.key().toLong() * 1000)))
		}

		override fun close() {
		}
	}

	private class HeaderToKeyExtractor<Key>(private val stateStoreName: String) : Processor<Key, String, String, String> {
		private lateinit var context: ProcessorContext<String, String>
		private lateinit var store: KeyValueStore<String, Int>

		override fun init(context: ProcessorContext<String, String>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)
			store.put("min", Int.MAX_VALUE)
		}

		override fun process(record: Record<Key, String>) {
			val index = String(record.headers().headers("index").first().value()).toInt()
			if (store["min"] > index)
				store.put("min", index)

			context.forward(record.withKey(index.toString()))
		}

		override fun close() {
		}
	}

	private class MinValueFilter<T>(private val stateStoreName: String) : FixedKeyProcessor<String, T, T> {
		private lateinit var context: FixedKeyProcessorContext<String, T>
		private lateinit var store: KeyValueStore<String, Int>

		override fun init(context: FixedKeyProcessorContext<String, T>) {
			this.context = context
			this.store = context.getStateStore(stateStoreName)
		}

		override fun process(record: FixedKeyRecord<String, T>) {
			if (record.key().toInt() == store["min"])
				context.forward(record)
		}

		override fun close() {
		}
	}
}
