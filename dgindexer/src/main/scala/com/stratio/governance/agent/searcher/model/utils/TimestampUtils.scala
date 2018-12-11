package com.stratio.governance.agent.searcher.model.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat

object TimestampUtils {

  val DEFAULT_PATTERN: String = "yyyy-MM-dd'T'HH:mm:ss.SSS"
  val MIN: Timestamp = TimestampUtils.fromString("1970-01-01T00:00:00.000")

  def fromString(date: String): Timestamp =
    new Timestamp(new SimpleDateFormat(DEFAULT_PATTERN).parse(date).getTime)

  def toString(timestamp: Timestamp): String =
    new SimpleDateFormat(DEFAULT_PATTERN).format(timestamp)

  def toSQLString(timestamp: Timestamp): String =
      s"'${toString(timestamp)}'"

  def toLong(timestamp: Timestamp): Long = timestamp.getTime

  def fromLong(time: Long): Timestamp =
    new Timestamp(time)

  @scala.annotation.tailrec
  def max(list: List[Timestamp]): Option[Timestamp] = list match {
    case Nil => None
    case List(x: Timestamp) => Some(x)
    case x :: y :: rest => max( (if (x.getTime > y.getTime) x else y) :: rest )
  }
}
