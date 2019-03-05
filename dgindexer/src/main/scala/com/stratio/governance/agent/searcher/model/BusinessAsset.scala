package com.stratio.governance.agent.searcher.model

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils

object BusinessType extends Enumeration {
  val TERM: BusinessType.Value = Value

  def fromString(value: String): BusinessType.Value = {
    BusinessType.withName(value)
  }
}

object BusinessStatus extends Enumeration {
  val APR, PEN, UNR: BusinessStatus.Value = Value

  def fromString(value: String): BusinessStatus.Value = {
    BusinessStatus.withName(value)
  }
}

case class BusinessAsset( metadataPath: String,
                          name: String,
                          description: String,
                          status: BusinessStatus.Value,
                          tpe: BusinessType.Value,
                          modifiedAt: Timestamp) extends EntityRow(metadataPath) {

  def this(metadataPath: String, name: String, description: String, status: String, tpe: String, modifiedAt: String) =
    this(metadataPath, name, description, BusinessStatus.fromString(status), BusinessType.fromString(tpe), TimestampUtils.fromString(modifiedAt))
}

object BusinessAsset {

  def apply(metadataPath: String, name: String, description: String, status: String, tpe: String, modifiedAt: String): BusinessAsset =
    new BusinessAsset(metadataPath, name, description, status, tpe, modifiedAt)

  @scala.annotation.tailrec
  def getValueFromResult(resultSet: ResultSet, list: List[BusinessAsset] = Nil): List[BusinessAsset] = {
    if (resultSet.next()) {
      val mod1: Timestamp = resultSet.getTimestamp(6)
      val mod2: Timestamp = resultSet.getTimestamp(7)
      val max = TimestampUtils.max(List(mod1, mod2))

      getValueFromResult(resultSet, BusinessAsset(resultSet.getString(1),
                                                  resultSet.getString(2),
                                                  resultSet.getString(3),
                                                  BusinessStatus.fromString(resultSet.getString(4)),
                                                  BusinessType.fromString(resultSet.getString(5)),
                                                  max.get) :: list)
    } else {
      list
    }
  }
}
