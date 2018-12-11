package com.stratio.governance.agent.searcher.model

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils

case class KeyValuePair(id: Int,
                        key: String,
                        value: String,
                        modifiedAt: Timestamp) extends EntityRow(id) {
  def this(id: Int, key: String, value: String, modifiedAt: String) = this (id, key, value, TimestampUtils.fromString(modifiedAt))
}

object KeyValuePair {

  def apply(id: Int, key: String, value: String, modifiedAt: String): KeyValuePair = new KeyValuePair(id, key, value, modifiedAt)

  @scala.annotation.tailrec
  def getValueFromResult(resultSet: ResultSet, list: List[KeyValuePair] = Nil): List[KeyValuePair] = {
    if (resultSet.next()) {
      val mod1 = resultSet.getTimestamp(4)
      val mod2 = resultSet.getTimestamp(5)
      val max = TimestampUtils.max(List(mod1, mod2))
      getValueFromResult(resultSet, KeyValuePair( resultSet.getInt(1),
                                                  resultSet.getString(2),
                                                  resultSet.getString(3),
                                                  max.get) :: list)
    } else {
      list
    }
  }
}
