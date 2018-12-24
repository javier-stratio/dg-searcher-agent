package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.actors.extractor.dao.SourceDao
import com.stratio.governance.agent.searcher.main.AppConf
import com.stratio.governance.agent.searcher.model.utils.{ExponentialBackOff, TimestampUtils}

case class PostgresPartialIndexationReadState(sourceDao: SourceDao) {

  var readDataAsset: Timestamp = TimestampUtils.MIN
  var readKeyDataAsset: Timestamp = TimestampUtils.MIN
  var readKey: Timestamp = TimestampUtils.MIN
  var readBusinessAssetsDataAsset: Timestamp = TimestampUtils.MIN
  var readBusinessAssets: Timestamp = TimestampUtils.MIN

  def read(connection: Connection): PostgresPartialIndexationReadState = {
    val preparedStatement: PreparedStatement = sourceDao.prepareStatement(PostgresPartialIndexationReadState.selectQuery)
    val resultSet: ResultSet = sourceDao.executeQueryPreparedStatement(preparedStatement)
    if (resultSet.next()) {
      readDataAsset = Option(resultSet.getTimestamp(1)).getOrElse(TimestampUtils.MIN)
      readKeyDataAsset = Option(resultSet.getTimestamp(2)).getOrElse(TimestampUtils.MIN)
      readKey = Option(resultSet.getTimestamp(3)).getOrElse(TimestampUtils.MIN)
      readBusinessAssetsDataAsset = Option(resultSet.getTimestamp(4)).getOrElse(TimestampUtils.MIN)
      readBusinessAssets = Option(resultSet.getTimestamp(5)).getOrElse(TimestampUtils.MIN)
    }

    // If it is the first read. Just insert
    if ( readDataAsset == TimestampUtils.MIN )
      insert(connection)

    this
  }

  private def insert(connection: Connection): Unit = {
    val preparedStatement: PreparedStatement = sourceDao.prepareStatement(PostgresPartialIndexationReadState.insertQuery)
    preparedStatement.setTimestamp(1, readDataAsset)
    preparedStatement.setTimestamp(2, readKeyDataAsset)
    preparedStatement.setTimestamp(3, readKey)
    preparedStatement.setTimestamp(4, readBusinessAssetsDataAsset)
    preparedStatement.setTimestamp(5, readBusinessAssets)
    sourceDao.executePreparedStatement(preparedStatement)
  }

  def update(connection: Connection): Unit = {
    val preparedStatement: PreparedStatement = sourceDao.prepareStatement(PostgresPartialIndexationReadState.updateQuery)
    preparedStatement.setTimestamp(1, readDataAsset)
    preparedStatement.setTimestamp(2, readKeyDataAsset)
    preparedStatement.setTimestamp(3, readKey)
    preparedStatement.setTimestamp(4, readBusinessAssetsDataAsset)
    preparedStatement.setTimestamp(5, readBusinessAssets)
    sourceDao.executePreparedStatement(preparedStatement)
  }

  def delete(connection: Connection): Unit = {
    sourceDao.executePreparedStatement(sourceDao.prepareStatement(PostgresPartialIndexationReadState.deleteQuery))
  }
}

object PostgresPartialIndexationReadState {
  private val schema: String = AppConf.sourceSchema
  private val table: String = "partial_indexation_state"
  val selectQuery: String = "SELECT last_read_data_asset, last_read_key_data_asset, last_read_key, " +
    s"last_read_business_assets_data_asset, last_read_business_assets FROM $schema.$table WHERE id = 1"
  val insertQuery: String = s"INSERT INTO $schema.$table VALUES (1, ?, ?, ?, ?, ?)"
  val updateQuery: String = s"UPDATE $schema.$table SET last_read_data_asset=?, last_read_key_data_asset=?, last_read_key=?, last_read_business_assets_data_asset=?, last_read_business_assets=? WHERE id = 1"
  val deleteQuery: String = s"DELETE FROM $schema.$table WHERE id = 1"
}