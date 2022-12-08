package org.sjoblomj.adventofcode.kafka

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler
import java.util.*

class KafkaStreamsSetup(topology: Topology) {
	private val kafkaStreams: KafkaStreams

	init {
		kafkaStreams = KafkaStreams(topology, kafkaConfig())
		kafkaStreams.start()
		Runtime.getRuntime().addShutdownHook(Thread(kafkaStreams::close))
	}


	private fun kafkaConfig() = Properties().also {
		it[StreamsConfig.APPLICATION_ID_CONFIG] = UUID.randomUUID().toString()
		it[StreamsConfig.BOOTSTRAP_SERVERS_CONFIG] = bootstrapServers
		it[StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.IntegerSerde::class.java
		it[StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.IntegerSerde::class.java
		it[StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG] = LogAndContinueExceptionHandler::class.java
		it[StreamsConfig.COMMIT_INTERVAL_MS_CONFIG] = 1000
	}


	fun close() {
		kafkaStreams.close()
		kafkaStreams.cleanUp()
	}
}
