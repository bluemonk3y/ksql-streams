package com.landoop.kstreams.sql.transform

import com.fasterxml.jackson.databind.JsonNode
import com.landoop.avro.sql.AvroSql._
import com.landoop.json.sql.JacksonJson
import com.landoop.json.sql.JsonSql._
import com.sksamuel.avro4s.{FromRecord, ScaleAndPrecision}
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericContainer, GenericRecord}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder}
import KStreamSAM._
import scala.reflect.ClassTag

/**
  * Offers the extension methods to your [[KStreamBuilder]] or [[KStream]]
  */
object KStreamSqlTransform {

  private def avroHandler[C](value: GenericContainer)(implicit fromRecord: FromRecord[C]): C = {
    value match {
      case gr: GenericRecord => fromRecord(gr)
      case n: NonRecordContainer => n.getValue.asInstanceOf[C]
      case null => null.asInstanceOf[C]
      case other => throw new IllegalArgumentException(s"${other.getClass.getCanonicalName} is not handled")
    }
  }

  implicit class KStreamBuilderExtensions(val streamBuilder: KStreamBuilder) extends AnyVal {

    /**
      * Translates the values of all messages received from Avro to a class (see avro4s restrictions)
      *
      * @param topics - The source for the KStream
      * @return An instance of KStream[T,C]
      */
    def mapAvroValueAsType[T, C](topics: String*)(implicit fromRecord: FromRecord[C]): KStream[T, C] = {
      val source: KStream[T, GenericContainer] = streamBuilder.stream(topics: _*)
      val stream: KStream[T, C] = source.mapValues { value: GenericContainer =>
        avroHandler(value)
      }
      stream
    }

    /**
      * Creates a KSource from the SQL applied and then maps the Kafka message value based on the query provided
      * The input SQL should be : SELECT ... FROM $topic
      *
      * @param sql - The sql selecting from a topic
      * @return
      */
    def mapAvroValue[T](sql: String): KStream[T, GenericContainer] = {
      mapAvroValue[T](Sql.parseSelect(sql))
    }

    /**
      * Creates a KSource from the SQL applied and then maps the Kafka message value based on the query provided
      * The context select should be the result of [[Sql.parseSelect]]
      *
      * @param context - Contains the transformation details.
      * @return
      */
    def mapAvroValue[T](context: SelectTransformContext): KStream[T, GenericContainer] = {
      val source: KStream[T, GenericContainer] = streamBuilder.stream(context.from)
      val stream: KStream[T, GenericContainer] = source.mapValues { value: GenericContainer => value.sql(context.fields, !context.withStructure) }
      stream
    }

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided to an instance of C
      * The sql parameter should be in the form of 'SELECT ... FROM $topic'
      *
      * @param sql - The select SQL
      * @return
      */
    def mapAvroValueAs[T, C](sql: String)(implicit fromRecord: FromRecord[C]): KStream[T, C] = {
      val context = Sql.parseSelect(sql)
      val source: KStream[T, GenericContainer] = streamBuilder.stream(context.from)
      val stream: KStream[T, C] = source.mapValues { value: GenericContainer =>
        value.sql(context.fields, !context.withStructure) match {
          case gr: GenericRecord => fromRecord(gr)
          case null => null.asInstanceOf[C]
          case n: NonRecordContainer => n.getValue.asInstanceOf[C]
          case other => throw new IllegalArgumentException(s"${other.getClass.getCanonicalName} is not handled")
        }
      }
      stream
    }

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided and send it to
      * the target topic. i.e. INSERT INTO $topicTarget SELECT * FROM $topicSource
      *
      *
      * Make sure both serdes are configured (i.e. avroserde has a reference to schema registry and is set as value serde)
      *
      * @param sql - The sql syntax driving the transformation: INSERT INTO $targetTopic SELECT ... FROM $sourceTopic
      * @return
      */
    def mapAvroValueTo[T](sql: String)(implicit key: Serde[T], avroSerde: AvroSerde): Unit = mapAvroValueTo[T](Sql.parseInsert(sql))


    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided and send it to
      * the target topic. i.e. INSERT INTO $topicTarget SELECT * FROM $topicSource
      *
      * Make sure both serdes are configured (i.e. avroserde has a reference to schema registry and is set as value serde)
      *
      * @param context - An instance of [[TransformContext]].
      * @return
      */
    def mapAvroValueTo[T](context: TransformContext)(implicit keySerde: Serde[T], avroSerde: AvroSerde): Unit = {
      val source: KStream[T, GenericContainer] = streamBuilder.stream(context.from)
      val stream: KStream[T, GenericContainer] = source.mapValues { value: GenericContainer =>
        value.sql(context.fields, !context.withStructure)
      }
      stream.to(keySerde, avroSerde, context.target)
    }

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided
      * *
      *
      * @param sql - The sql describing what to select from the Kafka message value: SELECT ... FROM $sourceTopic
      * @return
      */
    def mapJsonValue[T](sql: String): KStream[T, JsonNode] = mapJsonValue[T](Sql.parseSelect(sql))

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided
      *
      * @param context - An instance of [[SelectTransformContext]]
      * @return
      */
    def mapJsonValue[T](context: SelectTransformContext): KStream[T, JsonNode] = {
      val source: KStream[T, String] = streamBuilder.stream(context.from)
      val stream: KStream[T, JsonNode] = source.mapValues { value: String =>
        val json = JacksonJson.asJson(value)
        json.sql(context.fields, !context.withStructure)
      }
      stream
    }

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided to a instance of [[C]]
      * It is expected the Kafka message value is a json!
      *
      * @param sql - The sql describing what to pick from the message value: SELECT .. FROM $sourceTopic
      * @return
      */
    def mapJsonValueAs[T, C: ClassTag](sql: String): KStream[T, C] = {
      mapJsonValueAs[T, C](Sql.parseSelect(sql))
    }


    /**
      * Creates a KSource from the SQL applied and then converts the value based on the query provided to a instance of [[C]]
      * It is expected the Kafka message value is a json!
      *
      * @param context - An instance of [[SelectTransformContext]]
      * @return
      */
    def mapJsonValueAs[T, C: ClassTag](context: SelectTransformContext): KStream[T, C] = {
      val clazz = implicitly[ClassTag[C]].runtimeClass.asInstanceOf[Class[C]]
      val source: KStream[T, String] = streamBuilder.stream(context.from)
      val stream: KStream[T, C] = source.mapValues { value: String =>
        val json = JacksonJson.asJson(value)
        JacksonJson.mapper.treeToValue(json.sql(context.fields, !context.withStructure), clazz)
      }
      stream
    }

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided and sends it to
      * the target topic.
      *
      * It is expected the incoming Kafka message payload is a json. The output topic will contain the new transformed json messages
      *
      * @param sql - The sql syntax describing the transformation: INSERT INTO $topicTarget SELECT ... FROM $topicSource
      * @return
      */
    def mapJsonValueTo[T](sql: String): Unit = mapJsonValueTo[T](Sql.parseInsert(sql))

    /**
      * Creates a KSource from the SQL applied and then maps the value based on the query provided and sends it to
      * the target topic.
      *
      * It is expected the incoming Kafka message payload is a json. The output topic will contain the new transformed json messages
      *
      * @param context - An instance of the parsed SQL ([[TransformContext]])
      * @return
      */
    def mapJsonValueTo[T](context: TransformContext): Unit = {
      val source: KStream[T, String] = streamBuilder.stream(context.from)
      val stream: KStream[T, String] = source.mapValues { value: String =>
        val json = JacksonJson.asJson(value)
        json.sql(context.fields, !context.withStructure).toString
      }
      stream.to(context.target)
    }

    /**
      * Creates a stream reading from the provided topics and maps the message JSON value to avro
      * It is expected the incoming value is of type String
      *
      * @param avroName      - String providing the name of the record
      * @param avroNamespace - String that qualifies the avro name
      * @param topics        - The topics to read from
      * @tparam T
      * @return A KStream[K,GenericContainer]; GenericContainer can be NonRecordGeneric for primitive types (will work for a json containing arrays);
      *         otherwise it will be a GenericContainer
      */
    def mapJsonValueToAvro[T](avroName: String,
                              avroNamespace: String,
                              topics: String*)(implicit sp: ScaleAndPrecision): KStream[T, GenericContainer] = {
      require(avroName != null && avroName.trim.length > 0, "'name' can't be null or empty")
      val source: KStream[T, String] = streamBuilder.stream(topics: _*)
      val converter = new JsonToAvroConverter(avroNamespace)
      val stream: KStream[T, GenericContainer] = source.mapValues { value: String =>
        implicit val s: Option[Schema] = None
        converter.convert(avroName, value)
      }
      stream
    }

    /**
      * Maps the Json value of the Kafka message value to a instance of [[C]]
      *
      * @param topics - The Kafka topics to create the KSource from
      * @param tag    - Instance of [[ ClassTag[C] ]]
      * @tparam T - The Kafka message key type. You should set it according to the instance of [[KStreamBuilder]] you have setup
      * @tparam C - The target type to map the incoming Json
      * @return
      */
    def mapJsonValueAsType[T, C](topics: String*)(implicit tag: ClassTag[C]): KStream[T, C] = {
      val source: KStream[T, String] = streamBuilder.stream(topics: _*)
      val stream: KStream[T, C] = source.mapValues { value: String =>
        val c: C = JacksonJson.mapper.readValue[C](value, tag.runtimeClass.asInstanceOf[Class[C]])
        c
      }
      stream
    }
  }


  implicit class KStreamConverter[T](val kstream: KStream[T, GenericContainer]) extends AnyVal {

    /**
      * Maps the KStream value from an Avro record to an instance of [[C]].
      * If you are using Scala Product derived classes you don't have to provide the fromRecord instance.
      * Avro4s library does that work for you via macros
      *
      * @param fromRecord - An instance FromRecord providing the translation from Avro to C
      * @tparam C - The target type to map the Avro record to
      * @return
      */
    def mapAvroValueAs[C]()(implicit fromRecord: FromRecord[C]): KStream[T, C] = {
      kstream.mapValues { value: GenericContainer =>
        avroHandler(value)
      }
    }

    /**
      * Applies the SQL projection to the Avro record.
      * SQL example: SELECT *&#47;field1/field2.fieldA from A
      * The 'from A' although not used is required
      *
      * @param sql - The SQL instruction (i.e. SELECT ... FROM $topic)
      * @return
      */
    def mapAvroValue(sql: String): KStream[T, GenericContainer] = mapAvroValue(Sql.parseSelect(sql))

    /**
      * Applies the SQL projection to Kafka message value of type Avro record.
      * SQL example: SELECT *&#47;field1/field2.fieldA from A
      * The 'from A' although not used is required
      *
      * @param context - An instance of [[SelectTransformContext]]
      * @return
      */
    def mapAvroValue(context: SelectTransformContext): KStream[T, GenericContainer] = {
      kstream.mapValues { value: GenericContainer => value.sql(context.fields, !context.withStructure) }
    }
  }

  implicit class KStreamJsonConverter[T](val kstream: KStream[T, String]) extends AnyVal {

    /**
      * Maps the JSON String value to the an instance of C.
      *
      * @tparam C
      * @return
      */
    def mapJsonValueAs[C](clazz: Class[C]): KStream[T, C] = {
      kstream.mapValues { value: String =>
        JacksonJson.mapper.readValue(value, clazz)
      }
    }

    /**
      * Applies the SQL projection to the JSON record.
      * SQL example: SELECT *&#47;field1/field2.fieldA from A
      * The 'from A' although not used is required
      *
      * @param sql - The SQL transformation
      * @return
      */
    def mapJsonValue(sql: String): KStream[T, JsonNode] = mapJsonValue(Sql.parseSelect(sql))

    /**
      * Applies the SQL projection to the JSON record.
      * SQL example: SELECT *&#47;field1/field2.fieldA from A
      * The 'from A' although not used is required
      *
      * @param context - An instance of [[SelectTransformContext]]
      * @return
      */
    def mapJsonValue(context: SelectTransformContext): KStream[T, JsonNode] = {
      kstream.mapValues { value: String =>
        JacksonJson.asJson(value).sql(context.fields, !context.withStructure)
      }
    }
  }

}