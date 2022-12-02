package org.sjoblomj.adventofcode.kafka

import org.apache.kafka.common.serialization.Serdes

const val bootstrapServers = "localhost:29092"
const val resultTopic = "result"
const val part1 = "part1"
const val part2 = "part2"

val stringSerde = Serdes.StringSerde()
val intSerde = Serdes.IntegerSerde()
