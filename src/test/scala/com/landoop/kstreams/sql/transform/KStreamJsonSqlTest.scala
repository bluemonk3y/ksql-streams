package com.landoop.kstreams.sql.transform

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.{Properties, UUID}

import com.landoop.json.sql.JacksonJson
import com.landoop.kstreams.sql.cluster.KCluster
import com.landoop.kstreams.sql.transform.KStreamSAM._
import com.landoop.kstreams.sql.transform.KStreamSqlTransform._
import com.sksamuel.avro4s.ScaleAndPrecision
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericContainer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{Serdes, StringSerializer}
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

import scala.collection.mutable.ArrayBuffer

class KStreamJsonSqlTest extends WordSpec with Matchers with BeforeAndAfterAll {
  var cluster: KCluster = _

  override def beforeAll(): Unit = {
    cluster = new KCluster()
  }

  override def afterAll(): Unit = {
    cluster.close()
  }

  private def getStringStringProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", cluster.BrokersList)
    props.put("key.serializer", classOf[StringSerializer].getCanonicalName)
    props.put("value.serializer", classOf[StringSerializer].getCanonicalName)
    props
  }

  "KStreamSql" should {
    "translate from JSON to a Class" in {

      val fromTopic = "oders_in_to_class"
      val toTopic = "oders_out_to_class"
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, String](getStringStringProducerProps)

      val expectedRecords = Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      )

      expectedRecords.foreach { o =>
        val data = new ProducerRecord[String, String](fromTopic, o.toString, JacksonJson.toJson(o))
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      val stream: KStream[String, OrderInput] = builder.mapJsonValueAsType(fromTopic)
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

    "translate from String to Avro" in {
      val fromTopic = "oders_in_to_avro_ks" + System.currentTimeMillis()
      val toTopic = "oders_out_to_avro_ks" + System.currentTimeMillis()
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, String](getStringStringProducerProps)

      val expectedRecords = Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      )

      expectedRecords.foreach { o =>
        val data = new ProducerRecord[String, String](fromTopic, o.toString, JacksonJson.toJson(o))
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      implicit val sp = ScaleAndPrecision(18, 38)
      val streamAvro: KStream[String, GenericContainer] = builder.mapJsonValueToAvro("OrderInput", "test", fromTopic)
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

    "project JSON via sql" in {
      val fromTopic = "oders_in_sql_json" + System.currentTimeMillis()
      val toTopic = "orders_out_sql_json" + System.currentTimeMillis()
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, String](getStringStringProducerProps)

      val records = Seq(
        OrderInput(1, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 100.0),
        OrderInput(2, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 10.0),
        OrderInput(3, OrderInputMetadata(System.currentTimeMillis(), "peter", "buy"), 20.0)
      )

      records.foreach { o =>
        val data = new ProducerRecord[String, String](fromTopic, o.toString, JacksonJson.toJson(o))
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, cluster.SchemaRegistryService.get.Endpoint)

      val sql =
        s"""
           |SELECT i as id,
           |       metadata.`timestamp` as created,
           |       metadata.buySell,
           |       price
           |FROM $fromTopic""".stripMargin
      val stream: KStream[String, Order] = builder.mapJsonValueAs(sql)
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

    "project a JSON via sql while retaining the nested structure" in {
      val fromTopic = "oders_in_sql_json_with_structure" + System.currentTimeMillis()
      val toTopic = "orders_out_sql_json_with_Structure" + System.currentTimeMillis()
      cluster.createTopic(fromTopic)

      val producer = new KafkaProducer[String, String](getStringStringProducerProps)

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
            Ingredient("Emmenthaler", 1.5, 18),
            Ingredient("Parmesan", 4.5, 28),
            Ingredient("Tilsiter", 2.5, 16)),

          true,
          false,
          490)
      )

      records.foreach { pizza: Pizza =>
        val data = new ProducerRecord[String, String](fromTopic, pizza.toString, JacksonJson.toJson(pizza))
        producer.send(data)
      }

      val builder = new KStreamBuilder()

      val streamsConfiguration = new Properties()
      streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString)
      streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.BrokersList)
      streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
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

      val stream: KStream[String, LocalPizza] = builder.mapJsonValueAs(sql)
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
      actual.toList shouldBe expectedRecords
      topology.close()
    }
  }
}


