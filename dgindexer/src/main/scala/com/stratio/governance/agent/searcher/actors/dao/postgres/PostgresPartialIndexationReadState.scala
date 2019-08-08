package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.actors.extractor.dao.SourceDao
import com.stratio.governance.agent.searcher.model.utils.TimestampUtils
import org.slf4j.{Logger, LoggerFactory}

object PartialIndexationFields {
  val DATA_ASSET: String = "last_read_data_asset"
  val KEY_DATA_ASSET: String = "last_read_key_data_asset"
  val KEY: String = "last_read_key"
  val BUSINESS_ASSET_DATA_ASSET: String = "last_read_business_assets_data_asset"
  val BUSINESS_ASSET: String = "last_read_business_assets"
  val QUALITY_RULE: String = "last_read_quality_rules"
  val KEY_BUSINESS_ASSET: String = "last_read_key_business_assets"
  val KEY_QUALITY: String = "last_read_key_quality"
  val BUSINESS_ASSET_QUALITY: String = "last_read_business_assets_quality"
}

case class PostgresPartialIndexationReadState(sourceDao: SourceDao, schema: String) {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  var fields: Map[String, (Int, Timestamp)] = Map(
    (PartialIndexationFields.DATA_ASSET,(1,TimestampUtils.MIN)),
    (PartialIndexationFields.KEY_DATA_ASSET,(2,TimestampUtils.MIN)),
    (PartialIndexationFields.KEY,(3,TimestampUtils.MIN)),
    (PartialIndexationFields.BUSINESS_ASSET_DATA_ASSET,(4,TimestampUtils.MIN)),
    (PartialIndexationFields.BUSINESS_ASSET,(5,TimestampUtils.MIN)),
    (PartialIndexationFields.QUALITY_RULE,(6,TimestampUtils.MIN)),
    (PartialIndexationFields.KEY_BUSINESS_ASSET,(7,TimestampUtils.MIN)),
    (PartialIndexationFields.KEY_QUALITY,(8,TimestampUtils.MIN)),
    (PartialIndexationFields.BUSINESS_ASSET_QUALITY,(9,TimestampUtils.MIN))
  )

  private val table: String = "partial_indexation_state"
  private val selectQuery: String = s"SELECT ${orderKeysToString(None)} FROM $schema.$table WHERE id = 1"
  private val insertQuery: String = s"INSERT INTO $schema.$table VALUES (1, ${fields.toList.map(e => "?").mkString(",")})"
  private val updateQuery: String = s"UPDATE $schema.$table SET ${orderKeysToString(Some("=?"))} WHERE id = 1"
  private val deleteQuery: String = s"DELETE FROM $schema.$table WHERE id = 1"

  def isDataAssetMetadataTableCreated: Boolean = {
    val result = sourceDao.executeQuery(s"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '$schema' AND table_name = '$table')")
    result.next()
    val exists = result.getBoolean("exists")
    LOG.debug("isDataAssetMetadataTableCreated: " + exists.toString)
    exists
  }

  def createDataAssetMetadataTable() : Unit = {
    LOG.debug( s"creating $schema.$table table ... " )
    sourceDao.execute( s"CREATE TABLE IF NOT EXISTS $schema.$table (id SMALLINT NOT NULL UNIQUE, ${orderKeysToString(Some(" TIMESTAMP"))}, CONSTRAINT pk_$table PRIMARY KEY (id))" )
    LOG.debug( s"table $schema.$table created!" )
  }

  def read(connection: Connection): PostgresPartialIndexationReadState = {
    val preparedStatement: PreparedStatement = sourceDao.prepareStatement(selectQuery)
    val resultSet: ResultSet = sourceDao.executeQueryPreparedStatement(preparedStatement)
    var anyResult: Boolean = false
    if (resultSet.next()) {
      anyResult = true
      fields = fields.mapValues[(Int, Timestamp)](v => (v._1, Option(resultSet.getTimestamp(v._1)).getOrElse(TimestampUtils.MIN)))
    }

    // If it is the first read. Just insert
    if ( !anyResult )
      insert(connection)

    this
  }

  private def insert(connection: Connection): Unit = {
    val preparedStatement: PreparedStatement = sourceDao.prepareStatement(insertQuery)
    orderFields.foreach(field => {
      preparedStatement.setTimestamp(field._2._1, field._2._2)
    })
    sourceDao.executePreparedStatement(preparedStatement)
  }

  def update(connection: Connection): Unit = {
    val preparedStatement: PreparedStatement = sourceDao.prepareStatement(updateQuery)
    orderFields.foreach(field => {
      preparedStatement.setTimestamp(field._2._1, field._2._2)
    })
    sourceDao.executePreparedStatement(preparedStatement)
  }

  def delete(connection: Connection): Unit = {
    sourceDao.executePreparedStatement(sourceDao.prepareStatement(deleteQuery))
  }

  def getTimeStamp(field: String): Timestamp = {
    fields.get(field) match {
      case Some(ts) => ts._2
      case None => throw new RuntimeException("field " + field + " not found")
    }
  }

  private def orderKeysToString(conc: Option[String]): String = {
    conc match {
      case Some(c) => fields.toList.sortWith(_._2._1 < _._2._1).map(_._1).map(_.concat(c)).mkString(",")
      case None => fields.toList.sortWith(_._2._1 < _._2._1).map(_._1).mkString(",")
    }
  }

  private def orderFields(): Map[String, (Int, Timestamp)] = {
    fields.toList.sortWith(_._2._1 < _._2._1).toMap
  }


}
