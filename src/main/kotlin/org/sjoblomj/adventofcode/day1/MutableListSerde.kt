package org.sjoblomj.adventofcode.day1

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class MutableListSerde : Serde<MutableList<Int>> {
	override fun configure(configs: Map<String, *>, isKey: Boolean) {}
	override fun close() {}
	override fun serializer() = StringsListSerde()
	override fun deserializer() = StringsListSerde()
}

class StringsListSerde : Serializer<MutableList<Int>>, Deserializer<MutableList<Int>> {
	private var mapper: ObjectMapper? = ObjectMapper()

	override fun configure(configs: Map<String, *>, isKey: Boolean) {
		if (mapper == null) {
			mapper = ObjectMapper()
		}
	}

	override fun serialize(topic: String, data: MutableList<Int>): ByteArray {
		return mapper!!.writeValueAsBytes(data)
	}

	override fun deserialize(topic: String, data: ByteArray?): MutableList<Int> {
		if (data == null) {
			return mutableListOf()
		}
		return mapper!!.readValue(data, mutableListOf<Int>().javaClass)
	}

	override fun close() {
		mapper = null
	}
}
