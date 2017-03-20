package com.landoop.kstreams.sql.transform

import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Properties, UUID}

import com.landoop.kstreams.sql.cluster.KCluster
import com.landoop.kstreams.sql.transform.KStreamSAM._
import com.landoop.kstreams.sql.transform.KStreamSqlTransform._
import com.sksamuel.avro4s.{FromRecord, RecordFormat}
import io.confluent.kafka.serializers.{AbstractKafkaAvroSerDeConfig, KafkaAvroDeserializer}
import org.apache.avro.generic.{GenericContainer, GenericRecord}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class KStreamAvroSqlTest extends WordSpec with Matchers with BeforeAndAfterAll {
  var cluster: KCluster = _

  override def beforeAll(): Unit = {
    cluster = new KCluster()
  }

  override def afterAll(): Unit = {
    cluster.close()
  }

  private def getStringAvroProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", cluster.BrokersList)
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)
    props
  }

  private def stringAvroConsumerProps(group: String = "stringAvroGroup"): Properties = {
    val props = new Properties
    props.put("bootstrap.servers", cluster.BrokersList)
    props.put("group.id", group)
    props.put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000")
    props.put("auto.commit.enabled", "false")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[KafkaAvroDeserializer])
    props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)
    props
  }

  def stringStringConsumerProps(group: String = "stringstringGroup"): Properties = {
    val props = new Properties
    props.put("bootstrap.servers", cluster.BrokersList)
    props.put("group.id", group)
    props.put("session.timeout.ms", "6000") // default value of group.min.session.timeout.ms.
    props.put("heartbeat.interval.ms", "2000")
    props.put("auto.commit.enabled", "false")
    props.put("auto.offset.reset", "earliest")
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[StringDeserializer])
    props
  }

  def consumeRecords[K, V](consumer: Consumer[K, V], topic: String): Iterator[ConsumerRecord[K, V]] = {
    consumer.subscribe(util.Arrays.asList(topic))
    val result = Iterator.continually {
      consumer.poll(1000)
    }.flatten
    result
  }

  "KStreamSql" should {
    "translate from Avro to a Class" in {

      val fromTopic = "oders_in_to_class"
      val toTopic = "oders_out_to_class"
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, GenericRecord](getStringAvroProducerProps)

      val expectedRecords = Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      )

      expectedRecords.foreach { o =>
        val data = new ProducerRecord[String, GenericRecord](fromTopic, o.toString, o.toAvro)
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      val stream: KStream[String, OrderInput] = builder.mapAvroValueAsType(fromTopic)
      val countdown = new CountDownLatch(expectedRecords.size)
      val actual = ArrayBuffer.empty[OrderInput]
      val dummy: KStream[String, Unit] = stream.mapValues { order: OrderInput =>
        actual += order
        countdown.countDown()
      }

      val topology: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
      topology.start()

      countdown.await(30, TimeUnit.SECONDS)
      actual shouldBe expectedRecords
      topology.close()
    }

    "translate from Avro to a Class using the extension to KStream" in {
      val fromTopic = "oders_in_to_class_ks"
      val toTopic = "oders_out_to_class_ks"
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, GenericRecord](getStringAvroProducerProps)

      val expectedRecords = Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      )

      expectedRecords.foreach { o =>
        val data = new ProducerRecord[String, GenericRecord](fromTopic, o.toString, o.toAvro)
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      val streamAvro: KStream[String, GenericContainer] = builder.mapAvroValue(s"SELECT * FROM $fromTopic WITHSTRUCTURE")
      val streamClass: KStream[String, OrderInput] = streamAvro.mapAvroValueAs()

      val countdown = new CountDownLatch(expectedRecords.size)
      val actual = ArrayBuffer.empty[OrderInput]
      val dummy: KStream[String, Unit] = streamClass.mapValues { order: OrderInput =>
        actual += order
        countdown.countDown()
      }

      val topology: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
      topology.start()

      countdown.await(30, TimeUnit.SECONDS)
      actual shouldBe expectedRecords
      topology.close()
    }

    "project an Avro via sql" in {
      val fromTopic = "oders_in_sql" + System.currentTimeMillis()
      val toTopic = "orders_out_sql" + System.currentTimeMillis()
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, GenericRecord](getStringAvroProducerProps)

      val records = Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      )

      records.foreach { o =>
        val data = new ProducerRecord[String, GenericRecord](fromTopic, o.toString, o.toAvro)
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      val sql =
        s"""
           |SELECT i as id,
           |       metadata.`timestamp` as created,
           |       metadata.buySell,
           |       price
           |FROM $fromTopic""".stripMargin
      val stream: KStream[String, Order] = builder.mapAvroValueAs(sql)
      val countdown = new CountDownLatch(records.size)
      val actual = ArrayBuffer.empty[Order]
      val dummy: KStream[String, Unit] = stream.mapValues { order: Order =>
        actual += order
        countdown.countDown()
      }

      val topology: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
      topology.start()

      countdown.await(30, TimeUnit.SECONDS)

      val expectedRecords = records.map { r =>
        Order(r.i, r.metadata.timestamp, r.metadata.buySell, r.price)
      }
      actual shouldBe expectedRecords
      topology.close()
    }

    "project an Avro via sql while retaining the nested structure" in {
      val fromTopic = "oders_in_sql_with_structure" + System.currentTimeMillis()
      val toTopic = "orders_out_sql_with_Structure" + System.currentTimeMillis()
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, GenericRecord](getStringAvroProducerProps)

      val records = Seq(
        Pizza("Pepperoni",
          Seq(
            Ingredient("pepperoni", 12, 4.4),
            Ingredient("onions", 1, 0.4)),
          false,
          false,
          98),
        Pizza("Quattro Formaggio",
          Seq(Ingredient("Gorgonzola", 9.1, 21.9),
            Ingredient("", 1.5, 18.3)),
          true,
          false,
          490)
      )

      val recordFormat = RecordFormat[Pizza]
      records.foreach { pizza: Pizza =>
        val data = new ProducerRecord[String, GenericRecord](fromTopic, pizza.toString, recordFormat.to(pizza))
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      val sql =
        s"""
           |SELECT
           |  name,
           |  ingredients.name as fieldName,
           |  calories as cals,
           |  ingredients.sugar as fieldSugar,
           |  ingredients.*
           |FROM $fromTopic
           |withstructure""".stripMargin

      implicit val fromRecord = FromRecord[LocalPizza]
      val stream: KStream[String, LocalPizza] = builder.mapAvroValueAs(sql)
      val countdown = new CountDownLatch(records.size)
      val actual = ArrayBuffer.empty[LocalPizza]
      val dummy: KStream[String, Unit] = stream.mapValues { pizza: LocalPizza =>
        actual += pizza
        countdown.countDown()
      }

      val topology: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
      topology.start()

      countdown.await(30, TimeUnit.SECONDS)

      val expectedRecords = records.map { r =>
        LocalPizza(r.name,
          r.ingredients.map { i =>
            LocalIngredient(i.name, i.sugar, i.fat)
          },
          r.calories)
      }
      actual shouldBe expectedRecords
      topology.close()
    }
  }
}