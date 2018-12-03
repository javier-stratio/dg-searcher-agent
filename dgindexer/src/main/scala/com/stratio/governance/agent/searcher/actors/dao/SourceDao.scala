package com.stratio.governance.agent.searcher.actors.dao

import java.time.Instant

import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao

abstract class SourceDao {

  def close():Unit

  def keyValuePairProcess(ids: Array[Int]): List[EntityRow]

  def businessTerms(ids: Array[Int]): List[EntityRow]

  def readDataAssetsSince(instant: Option[Instant], limit: Int) : (Array[DataAssetDao], Option[Instant])

  def readLastIngestedInstant(): Option[Instant]

  def writeLastIngestedInstant(instant: Option[Instant])
}
