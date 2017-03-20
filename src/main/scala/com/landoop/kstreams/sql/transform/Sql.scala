package com.landoop.kstreams.sql.transform

import com.landoop.sql.Field
import org.apache.calcite.config.Lex
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.sql.{SqlIdentifier, SqlInsert, SqlSelect}

import scala.util.{Failure, Success, Try}

object Sql {
  private val config = SqlParser.configBuilder
    .setLex(Lex.MYSQL)
    .setCaseSensitive(false)
    .setIdentifierMaxLength(250)
    .build

  def parseSelect(sql: String): SelectTransformContext = {
    val withStructure: Boolean = sql.trim.toLowerCase().endsWith("withstructure")
    val query = if (withStructure) {
      sql.trim.dropRight("withstructure".length)
    } else sql

    val parser = SqlParser.create(query, config)
    Try(parser.parseQuery()) match {
      case Failure(e) => throw new IllegalArgumentException(s"Query is not valid.Needs to be `SELECT ... FROM $$sourceTopic`.${e.getMessage}")
      case Success(select: SqlSelect) =>
        validate(select)
        SelectTransformContext(
          select.getFrom.asInstanceOf[SqlIdentifier].getSimple,
          Field.from(select),
          withStructure)

      case Success(other) =>
        throw new IllegalArgumentException("Invalid statement. Needs to be `SELECT ... FROM $sourceTopic`")
    }
  }

  def validate(select: SqlSelect): Unit = {
    require(select.getFrom.isInstanceOf[SqlIdentifier], s"${select.getFrom} is not valid.")
  }

  def parseInsert(sql: String): TransformContext = {
    val withStructure: Boolean = sql.trim.toLowerCase().endsWith("withstructure")
    val query = if (withStructure) {
      sql.trim.dropRight("withstructure".length)
    } else sql

    val parser = SqlParser.create(query, config)
    Try(parser.parseQuery()) match {
      case Failure(e) => throw new IllegalArgumentException(s"Query is not valid.Needs to be `INSERT INTO A SELECT ... FROM A`.${e.getMessage}")
      case Success(sqlInsert: SqlInsert) =>
        validate(sqlInsert)
        val target = sqlInsert.getTargetTable.asInstanceOf[SqlIdentifier].getSimple
        val sqlSource = sqlInsert.getSource.asInstanceOf[SqlSelect]
        TransformContext(target,
          sqlSource.getFrom.asInstanceOf[SqlIdentifier].getSimple,
          Field.from(sqlSource),
          withStructure)

      case Success(other) =>
        throw new IllegalArgumentException("Invalid statement. Needs to be `INSERT INTO A SELECT ... FROM A`")
    }
  }

  def validate(insert: SqlInsert): Unit = {
    require(insert != null, "Null instances are invalid")
    require(insert.getTargetTable.isInstanceOf[SqlIdentifier], "Invalid target specified")
    insert.getSource match {
      case select: SqlSelect =>
        validate(select)
      case other => throw new IllegalArgumentException("Invalid source. Needs to be a SELECT .. FROM A")
    }
  }
}

case class TransformContext(target: String, from: String, fields: Seq[Field], withStructure: Boolean) {
  require(target != null && target.nonEmpty, s"'$target' is not valid")
  require(from != null && from.nonEmpty, s"'$from' is not valid")
  require(fields != null && fields.nonEmpty, "You need to specify what fields you want to select")
}


case class SelectTransformContext(from: String, fields: Seq[Field], withStructure: Boolean) {
  require(from != null && from.nonEmpty, s"'$from' is not valid")
  require(fields != null && fields.nonEmpty, "You need to specify what fields you want to select")
}