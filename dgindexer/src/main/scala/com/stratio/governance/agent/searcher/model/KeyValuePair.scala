package com.stratio.governance.agent.searcher.model

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils

case class KeyValuePair(identifier: String,
                        key: String,
                        value: String,
                        modifiedAt: Timestamp) extends EntityRow(identifier) {
  def this(identifier: String, key: String, value: String, modifiedAt: String) = this (identifier, key, value, TimestampUtils.fromString(modifiedAt))
}

object KeyValuePair {

  def apply(identifier: String, key: String, value: String, modifiedAt: String): KeyValuePair = new KeyValuePair(identifier, key, value, modifiedAt)

  @scala.annotation.tailrec
  def getValueFromResult(resultSet: ResultSet, list: List[KeyValuePair] = Nil): List[KeyValuePair] = {
    if (resultSet.next()) {
      val mod1 = resultSet.getTimestamp(4)
      val mod2 = resultSet.getTimestamp(5)
      val max = TimestampUtils.max(List(mod1, mod2))
      getValueFromResult(resultSet, KeyValuePair( resultSet.getString(1),
                                                  resultSet.getString(2),
                                                  resultSet.getString(3),
                                                  max.get) :: list)
    } else {
      list
    }
  }
}
