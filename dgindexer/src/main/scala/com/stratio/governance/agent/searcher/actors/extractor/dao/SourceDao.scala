package com.stratio.governance.agent.searcher.actors.extractor.dao

import java.sql.{PreparedStatement, ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresPartialIndexationReadState
import com.stratio.governance.agent.searcher.model.es.DataAssetES

trait SourceDao {

  def close():Unit

  def readDataAssetsSince(offset: Int, limit: Int): (Array[DataAssetES], Int)

  def readDataAssetsWhereIdsIn(ids: List[Int]): Array[DataAssetES]

  def readBusinessTermsWhereIdsIn(ids: List[Int]): Array[DataAssetES]

  def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], List[Int], PostgresPartialIndexationReadState)

  def readPartialIndexationState(): PostgresPartialIndexationReadState

  def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit

  def prepareStatement(queryName: String): PreparedStatement

  def execute(sql: String): Unit

  def executePreparedStatement(sql: PreparedStatement): Unit

  def executeQuery(sql: String): ResultSet

  def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet

}
