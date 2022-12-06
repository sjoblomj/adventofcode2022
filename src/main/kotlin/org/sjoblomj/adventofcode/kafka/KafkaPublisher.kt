package org.sjoblomj.adventofcode.kafka

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.File
import java.util.*
import java.util.concurrent.TimeUnit

class KafkaPublisher {

	private val mapper: ObjectMapper = ObjectMapper().also {
		it.enable(SerializationFeature.INDENT_OUTPUT)
		it.findAndRegisterModules()
	}
	private val producer = KafkaProducer<String?, String?>(HashMap<String, Any>().also {
		it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
		it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
		it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java
	})
	private val intProducer = KafkaProducer<Int, Int?>(HashMap<String, Any>().also {
		it[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
		it[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
		it[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = IntegerSerializer::class.java
	})


	fun readFile(fileName: String, topic: String) {
		var index = 0
		File(fileName).inputStream().bufferedReader().forEachLine {
			putDataOnTopic("${index++}", it, RecordHeaders(), topic, producer)
		}
	}

	fun putDataOnTopic(key: String?, value: String?, topic: String) {
		putDataOnTopic(key, value, RecordHeaders(), topic, producer)
	}

	fun <T> putDataOnTopic(key: String, value: T, topic: String) {
		putDataOnTopic(key, mapper.writeValueAsString(value), RecordHeaders(), topic, producer)
	}

	fun putDataOnTopic(key: Int, value: Int?, topic: String) {
		putDataOnTopic(key, value, RecordHeaders(), topic, intProducer)
	}

	private fun <K, V> putDataOnTopic(
		key: K?, value: V, headers: Headers, topic: String,
		kafkaProducer: KafkaProducer<K, V>
	): RecordMetadata {

		val producerRecord = ProducerRecord(topic, key, value)
		headers.add("MESSAGE_ID", UUID.randomUUID().toString().toByteArray())
		headers.forEach { h -> producerRecord.headers().add(h) }

		return kafkaProducer
			.send(producerRecord)
			.get(1000, TimeUnit.MILLISECONDS) // Blocking call
	}
}
