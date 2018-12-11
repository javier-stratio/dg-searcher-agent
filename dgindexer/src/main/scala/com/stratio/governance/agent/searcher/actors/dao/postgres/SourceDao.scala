package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.{PreparedStatement, ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.searcher.model.{BusinessAsset, KeyValuePair}

abstract class SourceDao {

  def close():Unit

  def keyValuePairProcess(ids: Array[Int]): List[KeyValuePair]

  def businessAssets(ids: Array[Int]): List[BusinessAsset]

  def readDataAssetsSince(timestamp: Timestamp, limit: Int): (Array[DataAssetES], Timestamp)

  def readDataAssetsWhereIdsIn(ids: List[Int]): Array[DataAssetES]

  def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], PostgresPartialIndexationReadState)

  def readPartialIndexationState(): PostgresPartialIndexationReadState

  def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit

  def prepareStatement(queryName: String): PreparedStatement

  def executeQuery(sql: String): ResultSet

  def executePreparedStatement(sql: PreparedStatement): ResultSet

}
