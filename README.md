[![Build Status](https://travis-ci.org/Landoop/kstreams-sql-transform.svg?branch=master)](https://travis-ci.org/Landoop/kstreams-sql-transform) 
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.landoop/kstreams-sql-transform_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.landoop/kstreams-sql-transform_2.11)
[![GitHub license](https://img.shields.io/github/license/Landoop/kstreams-sql-transform.svg)]()

# Kafka KStreams-Sql-Transform
Use SQL to project your Kafka message value layout when using Kafka Streaming.
The lib relies on [Landoop avro-sql](https://github.com/landoop/avro-sql) and [Landoop json-sql](https://github.com/landoop/json-sql)
while the SQL parsing is handled via **Apache Calcite**, therefore you have the option to address nested fields and in the future
apply filtering via *Where* and use expressions (string operations, number operations).


Through the API you can
* transform the JSON/AVRO payload using 
* translate an incoming JSON Kafka message to an AVRO one
* translate the incoming AVRO to a POJO (for Scala Product derived classes no work is required thanks to [avro4s!!](https://github.com/sksamuel/avro4s) 
For all other classes you need to provide a FromRecord implementation)
* translate the incoming JSON to a POJO

Quick content transformation can be achieved via SQL in a simple API call `mapAvroValueTo`:
```scala
import KStreamSqlTransform._ 

val toTopic = "SOME_TARGET"
val fromTopic = "SOME_SOURCE"
val sql =
s"""
   |INSERT INTO $toTopic
   |SELECT
   |  name,
   |  ingredients.name as fieldName,
   |  calories as cals,
   |  ingredients.sugar as fieldSugar,
   |  ingredients.*
   |FROM $fromTopic
   |withstructure""".stripMargin

builder.mapAvroValueTo(sql)

val topology: KafkaStreams = new KafkaStreams(builder, streamsConfiguration)
topology.start()
```
This will read all the messages from source and push them to the target topic while performing the content projection.
Simple!

## Release History

0.1 - first cut (2017-04-23)

## How to use it

The API offers extension methods for both KStream and KStreamBuilder.

#### Translate the incoming Avro to a POJO

This type of transformation allows you to return a flatten structure.
```scala
import KStreamSqlTransform._ 

val streamsConfiguration = new Properties()
...

streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)


val builder = new KStreamBuilder()
val stream: KStream[String, OrderInput] = builder.mapAvroValueAsType(fromTopic)
```

There is also an extension method for the KStream class which can achieve the same:

```scala
val streamAvro: KStream[String, GenericContainer] = ...
val streamClass: KStream[String, OrderInput] = streamAvro.mapAvroValueAs()
```


#### Translate the shape of the incoming AVRO 

```scala
import KStreamSqlTransform._ 

val builder = new KStreamBuilder()

val streamsConfiguration = new Properties()
streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)

val sql =
s"""
   |SELECT i as id,
   |       metadata.timestamp as created,
   |       metadata.buySell,
   |       price
   |FROM $fromTopic""".stripMargin
val stream: KStream[String, Order] = builder.mapValueWithKcqlAvroAs(kcql)
```

You can translate the Avro without loading to another class like this:
```scala
val stream: KStream[String, GenericContainer] = builder.mapAvroValue(sql)
```


#### Translate the shape of the Avro record retaining the nested structure

You might want to be able to rename a field or drop some fields.

```scala
import KStreamSqlTransform._ 

val streamsConfiguration = new Properties()
streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde].getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)

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
val stream: KStream[String, LocalPizza] = builder.mapAvroValueAs(kcql)
```

Notice the `withstructure` keyword.  You can avoid loading to a POJO by using this:
```scala
val stream: KStream[String, GenericContainer] = builder.mapAvroValue(sql)
```


#### Translate a JSON payload to AVRO

```scala
import KStreamSqlTransform._ 

val builder = new KStreamBuilder()

val streamsConfiguration = new Properties()
streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)

implicit val sp = ScaleAndPrecision(18, 38)
val streamAvro: KStream[String, GenericContainer] = builder.mapJsonValueToAvro("OrderInput", "test", fromTopic)
val streamClass: KStream[String, OrderInput] = streamAvro.mapAvroValueAs()

```

#### Translate a JSON to a POJO

It is expected the Kafka message payload is JSON 
```scala
import KStreamSqlTransform._ 

val builder = new KStreamBuilder()

val streamsConfiguration = new Properties()
streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)

val stream: KStream[String, OrderInput] = builder.mapJsonValueAsType(fromTopic)
```

#### Transform the shape of the incoming JSON payload

```scala
import KStreamSqlTransform._ 

val builder = new KStreamBuilder()

val streamsConfiguration = new Properties()
streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)

val sql =
s"""
   |SELECT i as id,
   |       metadata.timestamp as created,
   |       metadata.buySell,
   |       price
   |FROM $fromTopic""".stripMargin
val stream: KStream[String, Order] = builder.mapJsonValue(sql)
```


#### Translate the shape of the JSON retaining the nested structure

```scala
import KStreamSqlTransform._ 

val builder = new KStreamBuilder()

val streamsConfiguration = new Properties()
streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ...)

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

val stream: KStream[String, LocalPizza] = builder.mapJsonValueAs(kcql)
```

The lib provides extension methods for KStream as well. If you import `KStreamSqlTransform` you will see the following
methods available
* mapAvroValuesAs
* mapAvroValue
* mapJsonValueAs
* mapJsonValue
### SQL

There are two types of queries you can apply to both JSON or AVRO:
* to flatten it
* to retain the structure while cherry-picking and/or renaming fields
The difference between the two is marked by the **_withstructure_*** keyword.
If this is missing you will end up flattening the structure.

Let's take a look at the flatten first. There are cases when you are receiving a nested
avro structure and you want to flatten the structure while being able to cherry pick the fields and rename them.
Imagine we have the following Avro schema (same goes for JSON):
```
{
  "type": "record",
  "name": "Person",
  "namespace": "com.landoop.sql.avro",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "address",
      "type": {
        "type": "record",
        "name": "Address",
        "fields": [
          {
            "name": "street",
            "type": {
              "type": "record",
              "name": "Street",
              "fields": [
                {
                  "name": "name",
                  "type": "string"
                }
              ]
            }
          },
          {
            "name": "street2",
            "type": [
              "null",
              "Street"
            ]
          },
          {
            "name": "city",
            "type": "string"
          },
          {
            "name": "state",
            "type": "string"
          },
          {
            "name": "zip",
            "type": "string"
          },
          {
            "name": "country",
            "type": "string"
          }
        ]
      }
    }
  ]
}
```
Applying this SQL like syntax
```
SELECT 
    name, 
    address.street.*, 
    address.street2.name as streetName2 
FROM topic
```
the projected new schema is:
```
{
  "type": "record",
  "name": "Person",
  "namespace": "com.landoop.sql.avro",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "name_1",
      "type": "string"
    },
    {
      "name": "streetName2",
      "type": "string"
    }
  ]
}
```

There are scenarios where you might want to rename fields and maybe reorder them.
By applying this SQL like syntax on the Pizza schema

```
SELECT 
       name, 
       ingredients.name as fieldName, 
       ingredients.sugar as fieldSugar, 
       ingredients.*, 
       calories as cals 
FROM topic 
withstructure
```
we end up projecting the first structure into this one:

```json
{
  "type": "record",
  "name": "Pizza",
  "namespace": "com.landoop.sql.avro",
  "fields": [
    {
      "name": "name",
      "type": "string"
    },
    {
      "name": "ingredients",
      "type": {
        "type": "array",
        "items": {
          "type": "record",
          "name": "Ingredient",
          "fields": [
            {
              "name": "fieldName",
              "type": "string"
            },
            {
              "name": "fieldSugar",
              "type": "double"
            },
            {
              "name": "fat",
              "type": "double"
            }
          ]
        }
      }
    },
    {
      "name": "cals",
      "type": "int"
    }
  ]
}
```

#### Flatten rules
* you can't flatten a schema containing array fields
* when flattening and the column name has already been used it will get a index appended. For example if field *name* appears twice and you don't specifically
rename the second instance (*name as renamedName*) the new schema will end up containing: *name* and *name_1*
