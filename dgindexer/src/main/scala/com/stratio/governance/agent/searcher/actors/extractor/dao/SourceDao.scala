package com.stratio.governance.agent.searcher.actors.extractor.dao

import java.sql.{PreparedStatement, ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.model.es.DataAssetES

trait SourceDao {

  def close():Unit

  def readDataAssetsSince(timestamp: Timestamp, limit: Int): (Array[DataAssetES], Timestamp)

  def readDataAssetsWhereIdsIn(ids: List[Int]): Array[DataAssetES]

  def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], PostgresPartialIndexationReadState)

  def readPartialIndexationState(): PostgresPartialIndexationReadState

  def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit

  def prepareStatement(queryName: String): PreparedStatement

  def executeQuery(sql: String): ResultSet

  def executePreparedStatement(sql: PreparedStatement): ResultSet

}