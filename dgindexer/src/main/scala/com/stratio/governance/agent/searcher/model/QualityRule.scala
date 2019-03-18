package com.stratio.governance.agent.searcher.model

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils

case class QualityRule(metadataPath: String,
                       name: String,
                       modifiedAt: Timestamp) extends EntityRow(metadataPath) {
}

object QualityRule {

  def apply(metadataPath: String, name: String, modifiedAt: String): QualityRule =
    new QualityRule(metadataPath, name, TimestampUtils.fromString(modifiedAt))

  @scala.annotation.tailrec
  def getValueFromResult(resultSet: ResultSet, list: List[QualityRule] = Nil): List[QualityRule] = {
    if (resultSet.next()) {
      getValueFromResult(resultSet, QualityRule(resultSet.getString(1),resultSet.getString(2),resultSet.getTimestamp(3)) :: list)
    } else {
      list
    }
  }
}