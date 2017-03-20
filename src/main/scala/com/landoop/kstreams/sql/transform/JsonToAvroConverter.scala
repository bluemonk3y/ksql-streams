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

import com.fasterxml.jackson.databind.node._
import com.sksamuel.avro4s.ScaleAndPrecision
import io.confluent.kafka.serializers
import io.confluent.kafka.serializers.NonRecordContainer
import org.apache.avro.generic.GenericContainer
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.{LogicalTypes, Schema}

class JsonToAvroConverter(namespace: String, avroStringTypeIsString: Boolean = false) {

  import org.json4s._
  import org.json4s.native.JsonMethods._

  def convert(name: String, str: String)
             (implicit schema: Option[Schema], sp: ScaleAndPrecision): GenericContainer = convert(name, parse(str))

  def convert(name: String, value: JValue)
             (implicit aggregatedSchema: Option[Schema], sp: ScaleAndPrecision): GenericContainer = {
    value match {
      case JArray(arr) =>
        val values = new java.util.ArrayList[AnyRef]()
        val prevSchema = aggregatedSchema.map(_.getField(name)).map(_.schema)
        val result = convert(name, arr.head)(prevSchema, sp)
        result match {
          case n: NonRecordContainer => values.add(n.getValue)
          case _ => values.add(result)
        }
        arr.tail.foreach { v =>
          convert(name, v)(prevSchema, sp) match {
            case n: NonRecordContainer => values.add(n.getValue)
            case other => values.add(other)
          }
        }

        new NonRecordContainer(Schema.createArray(result.getSchema), values)
      case JBool(b) =>
        new NonRecordContainer(Schema.create(Schema.Type.BOOLEAN), b)
      case JDecimal(d) =>
        val schema = Schema.create(Schema.Type.BYTES)
        val decimal = LogicalTypes.decimal(sp.precision, sp.scale)
        decimal.addToSchema(schema)

        new serializers.NonRecordContainer(schema, d.bigDecimal.unscaledValue().toByteArray)
      case JDouble(d) =>
        new serializers.NonRecordContainer(Schema.create(Schema.Type.DOUBLE), d)
      case JInt(i) =>
        new serializers.NonRecordContainer(Schema.create(Schema.Type.LONG), i.toLong)
      case JLong(l) =>
        new serializers.NonRecordContainer(Schema.create(Schema.Type.LONG), l)
      case JNothing =>
        new NonRecordContainer(Schema.create(Schema.Type.NULL), null)
      case JNull =>
        val schema = Schema.createUnion(java.util.Arrays.asList(Schema.create(Schema.Type.NULL), createStringSchema))
        new serializers.NonRecordContainer(schema, null)
      case JString(s) =>
        val schema = createStringSchema
        new serializers.NonRecordContainer(schema, s)
      case JObject(values) =>
        val schema = Schema.createRecord(name, "", namespace, false)
        val fields = new util.ArrayList[Schema.Field]()
        val default: AnyRef = null
        val fieldsMap = values.map { case (n, v) =>
          val prevSchema = aggregatedSchema.map(_.getField(n)).map(_.schema())
          val result = convert(n, v)(prevSchema, sp)

          //schema.setFields(java.util.Arrays.asList()))
          fields.add(new Schema.Field(n, result.getSchema, "", default))
          n -> result
        }.toMap

        import scala.collection.JavaConversions._
        aggregatedSchema
          .foreach { schema =>
            schema.getFields
              .withFilter(f => !fieldsMap.contains(f.name()))
              .foreach { f =>
                fields.add(new Schema.Field(f.name(), f.schema(), "", default))
              }
          }

        schema.setFields(fields)
        val record = new Record(schema)
        fieldsMap.foreach {
          case (field, v: NonRecordContainer) => record.put(field, v.getValue)
          case (field, v: GenericContainer) => record.put(field, v)
        }

        record
    }
  }

  private def createStringSchema = {
    val schema = Schema.create(Schema.Type.STRING)
    if (avroStringTypeIsString) schema.addProp("avro.java.string", new TextNode("String"))
    schema
  }
}

