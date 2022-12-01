package org.sjoblomj.adventofcode.kafka

import org.apache.kafka.common.serialization.Serdes

const val bootstrapServers = "localhost:29092"
val stringSerde = Serdes.StringSerde()
