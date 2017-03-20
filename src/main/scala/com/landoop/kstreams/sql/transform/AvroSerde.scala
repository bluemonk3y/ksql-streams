/*
 * Copyright 2017 Landoop.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.landoop.kstreams.sql.transform

import java.util

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

class AvroSerde private(inner:Serde[GenericContainer]) extends Serde[GenericContainer] {
  def this() = this(Serdes.serdeFrom(new AvroSerializer(), new AvroDeserializer()))

  def this(client: SchemaRegistryClient) = {
    this(Serdes.serdeFrom(new AvroSerializer(client), new AvroDeserializer(client)))
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    inner.serializer().configure(configs, isKey)
    inner.deserializer().configure(configs, isKey)
  }

  override def serializer(): Serializer[GenericContainer] = inner.serializer()

  override def deserializer(): Deserializer[GenericContainer] = inner.deserializer()

  override def close(): Unit = inner.close()
}

class AvroSerializer(val inner:KafkaAvroSerializer) extends Serializer[GenericContainer] {
  def this() = this(new KafkaAvroSerializer)

  def this(client: SchemaRegistryClient) = this(new KafkaAvroSerializer(client))

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = inner.configure(configs, isKey)

  override def serialize(topic: String, data: GenericContainer): Array[Byte] = inner.serialize(topic,data)

  override def close(): Unit = inner.close()
}


class AvroDeserializer(val inner:KafkaAvroDeserializer) extends Deserializer[GenericContainer] {
  def this() = this(new serializers.KafkaAvroDeserializer())

  def this(client: SchemaRegistryClient) = this(new serializers.KafkaAvroDeserializer(client))

  override def close(): Unit = inner.close()

  override def deserialize(topic: String, data: Array[Byte]): GenericContainer = {
    inner.deserialize(topic, data).asInstanceOf[GenericContainer]
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = inner.configure(configs, isKey)
}
