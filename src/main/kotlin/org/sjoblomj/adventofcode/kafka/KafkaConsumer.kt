package org.sjoblomj.adventofcode.kafka

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.*

private val logger = LoggerFactory.getLogger("org.sjoblomj.adventofcode.kafka.KafkaConsumer")

fun getAllRecords(topic: String, expectedKeys: List<String> = emptyList(), maxWaitTime: Long = 10_000)
: List<ConsumerRecord<String, String>> {

	val startTime = System.currentTimeMillis()

	KafkaConsumer<String, String>(kafkaConfig())
		.use {
			it.subscribe(listOf(topic))

			val recs = consumeRecords(it, maxWaitTime, expectedKeys)

			val timeTaken = System.currentTimeMillis() - startTime
			logger.info("For topic $topic: Found ${recs.size} records in $timeTaken ms")
			return recs
		}
}

private fun consumeRecords(consumer: KafkaConsumer<String, String>, maxTime: Long, expectedKeys: List<String>)
: List<ConsumerRecord<String, String>> {

	val startTime = System.currentTimeMillis()
	val recs = mutableListOf<ConsumerRecord<String, String>>()

	while ((recs.isEmpty() || !recs.map { it.key() }.containsAll(expectedKeys)) && System.currentTimeMillis() - startTime < maxTime) {
		val records = consumer.poll(Duration.ofMillis(maxTime))
		for (record in records)
			if (record.value() != null)
				recs.add(record)
	}
	return recs
}

private fun kafkaConfig() = Properties().also {
	it[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = "earliest"
	it[ConsumerConfig.GROUP_ID_CONFIG] = "consumer"
	it[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
	it[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
	it[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
}
