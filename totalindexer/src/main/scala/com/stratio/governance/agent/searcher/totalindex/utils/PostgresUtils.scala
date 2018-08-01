package com.stratio.governance.agent.searcher.totalindex.utils

import java.sql.ResultSet
import java.util.Date

import play.api.libs.json._

import scala.annotation.tailrec

object PostgresUtils {

  def sqlResultSetToJson(rs: ResultSet): JsArray = {
    // This is loosely ported from https://gist.github.com/kdonald/2137988

    val rsmd        = rs.getMetaData
    val columnCount = rsmd.getColumnCount

    // It may be faster to collect each line into a Seq or other iterable
    // and pass that to Json.arr() at the end.
    var qJsonArray: JsArray = Json.arr()
    while (rs.next) {
      var index = 1

      var rsJson: JsObject = Json.obj()
      while (index <= columnCount) {
        // Unfortunately jdbc ResultSetMetaData doesn't expose a reliable
        // getTableName method.  It returns the "originalTableName" which doesn't
        // include table aliases defined in the SELECT statement.
        // Therefore the table name needs to be hard coded into each column
        // name in the SELECT command.
        //
        // We should also be checking that there are no duplicate columnLabel's
        // The Json constructors will just mindlessly append items with dup names
        // to the JsObject.
        val column      = rsmd.getColumnLabel(index)
        val columnLabel = column.toLowerCase()

        val value = rs.getObject(column)
        if (value == null) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> JsNull
          )
        } else if (value.isInstanceOf[Integer]) {
          println(value.asInstanceOf[Integer])
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[Int]
          )
        } else if (value.isInstanceOf[String]) {
          println(value.asInstanceOf[String])
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[String]
          )
        } else if (value.isInstanceOf[Boolean]) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[Boolean]
          )
        } else if (value.isInstanceOf[Date]) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[Date].getTime
          )
        } else if (value.isInstanceOf[Long]) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[Long]
          )
        } else if (value.isInstanceOf[Double]) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[Double]
          )
        } else if (value.isInstanceOf[Float]) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[Float]
          )
        } else if (value.isInstanceOf[BigDecimal]) {
          rsJson = rsJson ++ Json.obj(
            columnLabel -> value.asInstanceOf[BigDecimal]
          )
          // } else if (value.isInstanceOf[Byte]) {
          //   rsJson = rsJson ++ Json.obj(
          //     columnLabel -> value.asInstanceOf[Byte]
          //   )
          // } else if (value.isInstanceOf[Array[Byte]]) {
          //   rsJson = rsJson ++ Json.obj(
          //     columnLabel -> value.asInstanceOf[Array[Byte]]
          //   )
        } else {
          throw new IllegalArgumentException("Unmappable object type: " + value.getClass)
        }
        index += 1
      }
      qJsonArray = qJsonArray :+ rsJson
    }
    qJsonArray
  }

  /*
  @tailrec
  def processCursor(query: => ResultSet)(process: ResultSet => Unit): Unit = {
    @tailrec
    def loop(resultSet: ResultSet,
             processed: Boolean): Boolean = {
      if (!resultSet.next()) processed
      else {
        process
        loop(resultSet, true)
      }
    }
    if (loop(query, false)) processCursor(query)(process)
  }
   */

  def processCursor[T](resultSet: ResultSet)(process: ResultSet => T) = {
    @tailrec
    def loop(seq: Seq[T], resultSet: ResultSet): Seq[T] = {
      if (resultSet.next()) loop(seq :+ process(resultSet), resultSet)
      else seq
    }
    loop(Seq.empty, resultSet)
  }
}
