package com.stratio.governance.agent.searcher.actors.dao

import java.sql.{Connection, PreparedStatement, Timestamp}
import java.time.Instant

import akka.util.Timeout
import com.stratio.governance.agent.searcher.model.EntityRow
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.commons.agent.domain.dao.DataAssetDao
import org.apache.commons.dbcp.{DelegatingConnection, PoolingDataSource}
import org.json4s.DefaultFormats
import org.postgresql.PGConnection
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings, DB}

import scala.concurrent.duration.MILLISECONDS

class PostgresSourceDao(sourceConnectionUrl: String, sourceConnectionUser: String, sourceConnectionPassword: String, sourceDatabase:String, initialSize: Int, maxSize: Int) extends SourceDao {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(sourceConnectionUrl, sourceConnectionUser, sourceConnectionPassword, ConnectionPoolSettings(initialSize, maxSize))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(false)

  val connection: Connection = ConnectionPool.borrow()
  val db: DB = DB(connection)
  val pgConnection: PGConnection = connection.asInstanceOf[DelegatingConnection].getInnermostDelegate.asInstanceOf[PGConnection]

  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)

  override def close():Unit = {
    db.close()
  }

  override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = ???

  override def businessTerms(ids: Array[Int]): List[EntityRow] = ???

  val selectFromDataAssetWithWhere: String = s"SELECT * FROM $sourceDatabase.data_asset WHERE modified_at >= ? LIMIT ?"
  val selectFromDataAssetWithWhereStatement: PreparedStatement = connection.prepareStatement(selectFromDataAssetWithWhere)

  val selectFromDataAsset: String = s"SELECT * FROM $sourceDatabase.data_asset LIMIT ?"
  val selectFromDataAssetStatement: PreparedStatement = connection.prepareStatement(selectFromDataAsset)

  override def readDataAssetsSince(instant: Option[Instant], limit: Int): (Array[DataAssetDao], Option[Instant]) = {
    selectFromDataAssetStatement.clearParameters()
    val resultSet = instant match {
      case Some(_) => {
        selectFromDataAssetWithWhereStatement.setTimestamp(0,Timestamp.from(instant.get))
        selectFromDataAssetWithWhereStatement.setInt(1, limit)
        selectFromDataAssetWithWhereStatement.executeQuery()
      }
      case None => {
        selectFromDataAssetStatement.setInt(0, limit)
        selectFromDataAssetStatement.executeQuery()
      }
    }

    val lista = DataAssetES.getDataAssetFromResult(resultSet)
    //TODO asumimos que nos da la lista ordenada por fecha hacer test
    (lista.toArray, lista.lastOption.map(_.modifiedAt.toInstant))
  }

  val selectFromLastIngestedInstant: String = s"SELECT * FROM $sourceDatabase.data_asset_last_ingested WHERE id = 1"
  val selectFromLastIngestedInstantStatement: PreparedStatement = connection.prepareStatement(selectFromLastIngestedInstant)

  override def readLastIngestedInstant(): Option[Instant] = {
    val resultSet = selectFromLastIngestedInstantStatement.executeQuery()
    if (resultSet.next()) {
      Some(resultSet.getTimestamp(1).toInstant)
    } else {
      None
    }
  }

  val insertIntoLastIngestedInstant: String = s"INSERT INTO $sourceDatabase.data_asset_last_ingested VALUES (1, ?)"
  val insertIntoLastIngestedInstantStatement: PreparedStatement = connection.prepareStatement(selectFromLastIngestedInstant)

  override def writeLastIngestedInstant(instant: Option[Instant]): Unit = instant match {
    case Some(_) =>
      insertIntoLastIngestedInstantStatement.setTimestamp(0, Timestamp.from(instant.get))
      insertIntoLastIngestedInstantStatement.executeUpdate()
    case None =>
  }


}


