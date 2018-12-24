package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp}

//import collection.JavaConverters._

import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.manager.dao.{SourceDao => ManagerSourceDao}
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.searcher.model.{BusinessAsset, KeyValuePair}
import org.apache.commons.dbcp.PoolingDataSource
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

import scala.concurrent.duration.MILLISECONDS

class PostgresSourceDao(sourceConnectionUrl: String,
                        sourceConnectionUser: String,
                        sourceConnectionPassword: String,
                        database:String,
                        schema: String,
                        initialSize: Int,
                        maxSize: Int,
                        var exponentialBackOff: ExponentialBackOff,
                        allowedToCreateContext: Boolean = false) extends ExtractorSourceDao with IndexerSourceDao with ManagerSourceDao {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton(sourceConnectionUrl, sourceConnectionUser, sourceConnectionPassword, ConnectionPoolSettings(initialSize, maxSize))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(true)


  val initialExponentialBackOff: ExponentialBackOff = exponentialBackOff
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)

  private val dataAssetTable: String = "data_asset"
  private val keyDataAssetTable: String = "key_data_asset"
  private val keyTable: String = "key"
  private val businessAssetsDataAssetsTable: String = "business_assets_data_asset"
  private val businessAssetsTable: String = "business_assets"
  private val businessAssetsTypeTable: String = "business_assets_type"
  private val businessAssetsStatusTable: String = "business_assets_status"

  private val partialIndexationStateTable: String = "partial_indexation_state"

  private var connection: Connection = ConnectionPool.borrow()

  private var preparedStatements: Map[String, PreparedStatement] = Map[String, PreparedStatement]()


  def restartConnection(): Unit = {
    connection.commit()
    connection.close()
    connection = ConnectionPool.borrow()
    preparedStatements = Map[String, PreparedStatement]()
  }

  override def prepareStatement(query: String): PreparedStatement =
    preparedStatements.get(query) match {
      case None =>
        var preparedStatement: PreparedStatement = null
        try {
          preparedStatement = connection.prepareStatement(query)
          exponentialBackOff = initialExponentialBackOff
        } catch{
          case exception: SQLException =>
            // See https://www.postgresql.org/docs/current/errcodes-appendix.html
            Thread.sleep(exponentialBackOff.actualPause)
            if (exception.getSQLState.startsWith("08")) { // Problems with Connection
              restartConnection()
            }
            exponentialBackOff = exponentialBackOff.next
            preparedStatement = prepareStatement(query)
        }
        preparedStatements += (query -> preparedStatement)
        preparedStatements(query)
      case Some(ps) => ps
    }

  def executeQuery(sql: String): ResultSet = {
    try{
      val rs = connection.createStatement().executeQuery(sql)
      exponentialBackOff = initialExponentialBackOff
      rs
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executeQuery - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executeQuery(sql)
    }
  }

  def execute(sql: String): Unit = {
    try{
      val rs = connection.createStatement().execute(sql)
      exponentialBackOff = initialExponentialBackOff
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executeQuery - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        execute(sql)
    }
  }

  def executePreparedStatement(sql: PreparedStatement): Unit = {
    try{
      val rs = sql.execute()
      exponentialBackOff = initialExponentialBackOff
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executePreparedStatement - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executePreparedStatement(sql)
    }
  }

  def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet = {
    try{
      val rs = sql.executeQuery()
      exponentialBackOff = initialExponentialBackOff
      rs
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executePreparedStatement - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executeQueryPreparedStatement(sql)
    }
  }


  def executeUpdatePreparedStatement(sql: PreparedStatement): Unit = {
    try{
      val rs = sql.executeUpdate()
      exponentialBackOff = initialExponentialBackOff
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executePreparedStatement - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executeUpdatePreparedStatement(sql)
    }
  }

  private var status : Option[PostgresPartialIndexationReadState] = None
  start()

  private def start(): Unit = {
    // TODO Check this. We do not want to create de schema
    /*
    if (!isDatabaseCreated)
      if (allowedToCreateContext) createDatabase() else throw new IllegalStateException(s"Database $database is not created")

    useDatabase()

    if (!isSchemaCreated)
      if (allowedToCreateContext) createSchema() else throw new IllegalStateException(s"Schema $schema is not created")

    if (!isDataAssetTableCreated)
      if (allowedToCreateContext) createDataAssetTable() else throw new IllegalStateException(s"Table $schema.$dataAssetTable is not created")
    */

    if (!isDataAssetMetadataTableCreated) createDataAssetMetadataTable()
  }

  def close():Unit = {
    connection.close()
  }

  private def isDatabaseCreated: Boolean = {
    executeQuery(s"SELECT EXISTS(SELECT 1 from pg_database WHERE datname='$database')").getBoolean("exists")
  }

  private def createDatabase() : Unit = {
    executeQuery(s"CREATE DATABASE $database")
  }

  private def useDatabase(): Unit = {
    executeQuery(s"\\c $database")
  }

  private def isSchemaCreated: Boolean = {
    executeQuery(s"SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name = '$schema')").getBoolean("exists")
  }

  private def createSchema(): Unit = {
    executeQuery(s"CREATE SCHEMA $schema")
  }

  private def isDataAssetTableCreated: Boolean = {
    executeQuery(s"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '$schema' AND table_name = '$dataAssetTable')").getBoolean("exists")
  }

  private def createDataAssetTable() : Unit = {
    executeQuery(s"CREATE TABLE IF NOT EXISTS $schema.$dataAssetTable(id SERIAL, name TEXT,description TEXT," +
      s"metadata_path TEXT NOT NULL UNIQUE,type TEXT NOT NULL,subtype TEXT NOT NULL,tenant TEXT NOT NULL," +
      s"properties jsonb NOT NULL,active BOOLEAN NOT NULL,discovered_at TIMESTAMP NOT NULL," +
      s"modified_at TIMESTAMP NOT NULL,CONSTRAINT pk_data_asset PRIMARY KEY (id)," +
      s"CONSTRAINT u_data_asset_meta_data_path_tenant UNIQUE (metadata_path, tenant))")
  }

  private def isDataAssetMetadataTableCreated: Boolean = {
    val result = executeQuery(s"SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema = '$schema' AND table_name = '$partialIndexationStateTable')")
    result.next()
    result.getBoolean("exists")
  }

  private def createDataAssetMetadataTable() : Unit = {
    execute(s"CREATE TABLE IF NOT EXISTS $schema.$partialIndexationStateTable (id SMALLINT NOT NULL UNIQUE," +
      s"last_read_data_asset TIMESTAMP,last_read_key_data_asset TIMESTAMP,last_read_key TIMESTAMP,last_read_business_assets_data_asset TIMESTAMP, last_read_business_assets TIMESTAMP, CONSTRAINT pk_$partialIndexationStateTable PRIMARY KEY (id))")
  }

  def keyValuePairProcess(ids: Array[Int]): List[KeyValuePair] = {
    try {
      // TODO This query has a problem with java-scala array conversion
//      val selectKeyValuePairStatement: PreparedStatement = prepareStatement(s"SELECT key_asset.data_asset_id, key.key, key_asset.value, key.modified_at, key_asset.modified_at FROM $schema.$keyDataAssetTable AS key_asset, $schema.$keyTable AS key WHERE key.id=key_asset.key_id AND key_asset.data_asset_id IN (?)")
//      val pgIds = connection.createArrayOf("int4", ids.toList.asJava.toArray)
//      selectKeyValuePairStatement.setArray(1, pgIds)

      // Alternative Option
      val repl:  String = ids.map( id => "?") match {
        case q: Array[String] => q.mkString(",")
      }
      val selectKeyValuePairStatement: PreparedStatement = prepareStatement(s"SELECT key_asset.data_asset_id, key.key, key_asset.value, key.modified_at, key_asset.modified_at FROM $schema.$keyDataAssetTable AS key_asset, $schema.$keyTable AS key WHERE key.id=key_asset.key_id AND key_asset.data_asset_id IN ({{ids}})".replace("{{ids}}",repl))
      var index: Int = 0
      ids.foreach(id => {
        index+=1
        selectKeyValuePairStatement.setInt(index, id)
      })

      KeyValuePair.getValueFromResult(selectKeyValuePairStatement.executeQuery())
    } catch {
      case e: Throwable =>
        LOG.error("error while getting key-Value Pairs", e)
        List[KeyValuePair]()
    }
  }

  def businessAssets(ids: Array[Int]): List[BusinessAsset] = {
    try {
      // TODO This query has a problem with java-scala array conversion
//      val selectBusinessTermsStatement: PreparedStatement = prepareStatement(s"SELECT bus_assets.data_asset_id, bus_assets.name, bus_assets.description, bus_assets_status.name, " +
//        s"bus_assets_type.name, bus_assets_data_assets.modified_at, bus_assets.modified_at " +
//        s"FROM $schema.$businessAssetsDataAssetsTable AS bus_assets_data_assets, " +
//        s"$schema.$businessAssetsTable AS bus_assets, " +
//        s"$schema.$businessAssetsTypeTable AS bus_assets_type, " +
//        s"$schema.$businessAssetsStatusTable AS bus_assets_status " +
//        s"WHERE bus_assets_data_assets.data_asset_id IN(?)" +
//        "AND bus_assets.id=bus_assets_data_assets.business_assets_id " +
//        "AND bus_assets_type.id=bus_assets.business_assets_type_id " +
//        "AND bus_assets_status.id=bus_assets.business_assets_status_id")
//      selectBusinessTermsStatement.setString(1, ids.map(s => s"'$s'").toList.mkString(","))

      // Alternative Option
      val repl:  String = ids.map( id => "?") match {
        case q: Array[String] => q.mkString(",")
      }
      val selectBusinessTermsStatement: PreparedStatement = prepareStatement(s"SELECT bus_assets_data_assets.data_asset_id, bus_assets.name, bus_assets.description, bus_assets_status.name, " +
        s"bus_assets_type.name, bus_assets_data_assets.modified_at, bus_assets.modified_at " +
        s"FROM $schema.$businessAssetsDataAssetsTable AS bus_assets_data_assets, " +
        s"$schema.$businessAssetsTable AS bus_assets, " +
        s"$schema.$businessAssetsTypeTable AS bus_assets_type, " +
        s"$schema.$businessAssetsStatusTable AS bus_assets_status " +
        s"WHERE bus_assets_data_assets.data_asset_id IN({{ids}})".replace("{{ids}}",repl) +
        "AND bus_assets.id=bus_assets_data_assets.business_assets_id " +
        "AND bus_assets_type.id=bus_assets.business_assets_type_id " +
        "AND bus_assets_status.id=bus_assets.business_assets_status_id")
      var index: Int = 0
      ids.foreach(id => {
        index+=1
        selectBusinessTermsStatement.setInt(index, id)
      })
      BusinessAsset.getValueFromResult(executeQueryPreparedStatement(selectBusinessTermsStatement))
    } catch {
      case e: Throwable =>
        LOG.error("error while getting Business Assets", e)
        List[BusinessAsset]()
    }
  }

  def readDataAssetsSince(offset: Int, limit: Int): (Array[DataAssetES], Int) = {
    val selectFromDataAssetWithWhereStatement: PreparedStatement = prepareStatement(s"SELECT id,name,alias,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM $schema.$dataAssetTable WHERE active = ? order by id asc limit ? offset ?")
    selectFromDataAssetWithWhereStatement.setBoolean(1, true)
    selectFromDataAssetWithWhereStatement.setInt(2, limit)
    selectFromDataAssetWithWhereStatement.setInt(3, offset)
    val lista = DataAssetES.getValuesFromResult(executeQueryPreparedStatement(selectFromDataAssetWithWhereStatement))
    (lista.toArray, offset + limit)
  }

  def readDataAssetsWhereIdsIn(ids: List[Int]): Array[DataAssetES] = {
    if (ids.isEmpty) Array() else {
      // TODO This query has a problem with java-scala array conversion
//      val selectFromDataAssetWithIdsInStatement: PreparedStatement = prepareStatement(s"SELECT id,name,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM $schema.$dataAssetTable WHERE id IN(?)")
//      val pgIds = connection.createArrayOf("int4", ids.toList.asJava.toArray)
//      selectFromDataAssetWithIdsInStatement.setArray(1, pgIds)

      // Alternative Option
      val repl:  String = ids.map( id => "?") match {
        case q: List[String] => q.mkString(",")
      }
      val selectFromDataAssetWithIdsInStatement: PreparedStatement = prepareStatement(s"SELECT id,name,alias,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM $schema.$dataAssetTable WHERE id IN({{ids}})".replace("{{ids}}",repl))
      var index: Int = 0
      ids.foreach(id => {
        index+=1
        selectFromDataAssetWithIdsInStatement.setInt(index, id)
      })

      DataAssetES.getValuesFromResult(executeQueryPreparedStatement(selectFromDataAssetWithIdsInStatement)).toArray
    }
  }

  private case class Result (id: Int, timestamp: Timestamp, literal: Short)

  def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[Int], PostgresPartialIndexationReadState) = {
    val unionSelectUpdatedStatement: PreparedStatement =
        prepareStatement(s"(" +
                                 s" SELECT id,modified_at,1 FROM $schema.$dataAssetTable WHERE modified_at > ? " +
                                   "UNION " +
                                 s"SELECT data_asset_id,modified_at,2 FROM $schema.$keyDataAssetTable WHERE modified_at > ? " +
                                   "UNION " +
                                 s"SELECT key_data_asset.data_asset_id, key.modified_at,3 FROM $schema.$keyDataAssetTable AS key_data_asset, " +
                                    s"$schema.$keyTable AS key WHERE key.modified_at > ? " +
                                   "UNION " +
                                 s"SELECT data_asset_id,modified_at,4 FROM $schema.$businessAssetsDataAssetsTable WHERE modified_at > ? " +
                                   "UNION " +
                                 s"SELECT bus_assets_data_assets.data_asset_id, bus_assets.modified_at,5 FROM $schema.$businessAssetsDataAssetsTable AS bus_assets_data_assets, " +
                                    s"$schema.$businessAssetsTable AS bus_assets WHERE bus_assets.modified_at > ? " +
                                s") ORDER BY modified_at DESC;")
    unionSelectUpdatedStatement.setTimestamp(1,state.readDataAsset)
    unionSelectUpdatedStatement.setTimestamp(2,state.readKeyDataAsset)
    unionSelectUpdatedStatement.setTimestamp(3,state.readKey)
    unionSelectUpdatedStatement.setTimestamp(4,state.readBusinessAssetsDataAsset)
    unionSelectUpdatedStatement.setTimestamp(5,state.readBusinessAssets)
    val resultSet: ResultSet = executeQueryPreparedStatement(unionSelectUpdatedStatement)
    var list : List[Result] = List()
    while (resultSet.next()) {
      list = Result(resultSet.getInt(1), resultSet.getTimestamp(2), resultSet.getShort(3)) :: list
    }
    val ids = list.map(_.id).distinct
    list.filter(_.literal == 1).map(_.timestamp).sortWith(_.getTime > _.getTime).headOption match {
      case Some(t) => state.readDataAsset = t
      case None =>
    }
    list.filter(_.literal == 2).map(_.timestamp).sortWith(_.getTime > _.getTime).headOption match {
      case Some(t) => state.readKeyDataAsset = t
      case None =>
    }
    list.filter(_.literal == 3).map(_.timestamp).sortWith(_.getTime > _.getTime).headOption match {
      case Some(t) => state.readKey = t
      case None =>
    }
    list.filter(_.literal == 4).map(_.timestamp).sortWith(_.getTime > _.getTime).headOption match {
      case Some(t) => state.readBusinessAssetsDataAsset = t
      case None =>
    }
    list.filter(_.literal == 5).map(_.timestamp).sortWith(_.getTime > _.getTime).headOption match {
      case Some(t) => state.readBusinessAssets = t
      case None =>
    }
    (ids, state)
  }

  override def getKeys(): List[String] = {
    PostgresSourceDao.getKeysFromResult(executeQueryPreparedStatement(prepareStatement(s"SELECT DISTINCT key FROM $schema.$keyTable")))
  }

  def readPartialIndexationState(): PostgresPartialIndexationReadState = status.getOrElse({
    status = Some(PostgresPartialIndexationReadState(this))
    status.get.read(connection)
    status.get
  })

  def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = {
    status = Some(state)
    status.get.update(connection)
  }


}
object PostgresSourceDao {

  @scala.annotation.tailrec
  def getKeysFromResult(resultSet: ResultSet, list: List[String] = Nil): List[String] = {
    if (resultSet.next()) {
      getKeysFromResult(resultSet, resultSet.getString(1) :: list)
    } else {
      list
    }
  }
}


