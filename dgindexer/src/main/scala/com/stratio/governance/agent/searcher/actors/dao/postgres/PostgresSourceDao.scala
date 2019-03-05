package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp}

import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness

//import collection.JavaConverters._

import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.manager.dao.{SourceDao => ManagerSourceDao}
import com.stratio.governance.agent.searcher.model.es.DataAssetES
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.stratio.governance.agent.searcher.model.{BusinessAsset, KeyValuePair}
import org.json4s.DefaultFormats
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.MILLISECONDS

class PostgresSourceDao(sourceConnectionUrl: String,
                        sourceConnectionUser: String,
                        sourceConnectionPassword: String,
                        database:String,
                        schema: String,
                        initialSize: Int,
                        maxSize: Int,
                        var exponentialBackOff: ExponentialBackOff,
                        additionalBusiness: AdditionalBusiness,
                        allowedToCreateContext: Boolean = false) extends ExtractorSourceDao with IndexerSourceDao with ManagerSourceDao {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  // initialize JDBC driver & connection pool
  val datasource: Datasource = Datasource.getDataSource(sourceConnectionUrl, "org.postgresql.Driver", sourceConnectionUser, sourceConnectionPassword, maxSize)
  private var connection: Connection = datasource.getConnection

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

  private var preparedStatements: Map[String, PreparedStatement] = Map[String, PreparedStatement]()


  def restartConnection(): Unit = {
    connection.commit()
    connection.close()
    connection = datasource.getConnection
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

  protected def start(): Unit = {
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
    val exists = result.getBoolean("exists")
    LOG.debug("isDataAssetMetadataTableCreated: " + exists.toString)
    exists
  }

  private def createDataAssetMetadataTable() : Unit = {
    LOG.debug( s"creating $schema.$partialIndexationStateTable table ... " )
    execute( s"CREATE TABLE IF NOT EXISTS $schema.$partialIndexationStateTable (id SMALLINT NOT NULL UNIQUE," +
      s"last_read_data_asset TIMESTAMP,last_read_key_data_asset TIMESTAMP,last_read_key TIMESTAMP,last_read_business_assets_data_asset TIMESTAMP, last_read_business_assets TIMESTAMP, CONSTRAINT pk_$partialIndexationStateTable PRIMARY KEY (id))" )
    LOG.debug( s"table $schema.$partialIndexationStateTable created!" )
  }

  def keyValuePairProcess(mdps: List[String]): List[KeyValuePair] = {
    if (!mdps.isEmpty) {
      try {
        // TODO This query has a problem with java-scala array conversion
        //      val selectKeyValuePairStatement: PreparedStatement = prepareStatement(s"SELECT key_asset.data_asset_id, key.key, key_asset.value, key.modified_at, key_asset.modified_at FROM $schema.$keyDataAssetTable AS key_asset, $schema.$keyTable AS key WHERE key.id=key_asset.key_id AND key_asset.data_asset_id IN (?)")
        //      val pgIds = connection.createArrayOf("int4", ids.toList.asJava.toArray)
        //      selectKeyValuePairStatement.setArray(1, pgIds)

        // Alternative Option
        val repl: String = mdps.map( mdp => "?" ) match {
          case q: List[String] => q.mkString( "," )
        }
        val selectKeyValuePairStatement: PreparedStatement = prepareStatement( s"SELECT key_asset.metadata_path, key.key, key_asset.value, key.modified_at, key_asset.modified_at FROM $schema.$keyDataAssetTable AS key_asset, $schema.$keyTable AS key WHERE key.id=key_asset.key_id AND key_asset.metadata_path IN ({{mdps}})".replace( "{{mdps}}", repl ) )
        var index: Int = 0
        mdps.foreach( mdp => {
          index += 1
          selectKeyValuePairStatement.setString( index, mdp )
        } )

        KeyValuePair.getValueFromResult( selectKeyValuePairStatement.executeQuery() )
      } catch {
        case e: Throwable =>
          LOG.error( "error while getting key-Value Pairs", e )
          List[KeyValuePair]()
      }
    } else {
      LOG.debug("There is no ids to get key-Value Pairs")
      List[KeyValuePair]()
    }
  }

  def businessAssets(mdps: List[String]): List[BusinessAsset] = {
    if (!mdps.isEmpty) {
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
        val repl: String = mdps.map( mdp => "?" ) match {
          case q: List[String] => q.mkString( "," )
        }
        val selectBusinessTermsStatement: PreparedStatement = prepareStatement( s"SELECT bus_assets_data_assets.metadata_path, bus_assets.name, bus_assets.description, bus_assets_status.name, " +
          s"bus_assets_type.name, bus_assets_data_assets.modified_at, bus_assets.modified_at " +
          s"FROM $schema.$businessAssetsDataAssetsTable AS bus_assets_data_assets, " +
          s"$schema.$businessAssetsTable AS bus_assets, " +
          s"$schema.$businessAssetsTypeTable AS bus_assets_type, " +
          s"$schema.$businessAssetsStatusTable AS bus_assets_status " +
          s"WHERE bus_assets_data_assets.metadata_path IN({{mdps}})".replace( "{{mdps}}", repl ) +
          "AND bus_assets.id=bus_assets_data_assets.business_assets_id " +
          "AND bus_assets_type.id=bus_assets.business_assets_type_id " +
          "AND bus_assets_status.id=bus_assets.business_assets_status_id" )
        var index: Int = 0
        mdps.foreach( mdp => {
          index += 1
          selectBusinessTermsStatement.setString( index, mdp )
        } )
        BusinessAsset.getValueFromResult( executeQueryPreparedStatement( selectBusinessTermsStatement ) )
      } catch {
        case e: Throwable =>
          LOG.error( "error while getting Business Assets", e )
          List[BusinessAsset]()
      }
    } else {
      LOG.debug("There is no ids to get Business Asset")
      List[BusinessAsset]()
    }
  }

  def readDataAssetsSince(offset: Int, limit: Int): (Array[DataAssetES], Int) = {
    val selectFromDataAssetWithWhereStatement: PreparedStatement = prepareStatement(s"((SELECT id,name,alias,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM $schema.$dataAssetTable WHERE active = ?) union " + additionalBusiness.getBTTotalIndexationsubquery(schema, businessAssetsTable, businessAssetsTypeTable) + ") order by id asc, name asc limit ? offset ?")
    selectFromDataAssetWithWhereStatement.setBoolean(1, true)
    selectFromDataAssetWithWhereStatement.setInt(2, limit)
    selectFromDataAssetWithWhereStatement.setInt(3, offset)
    val list = DataAssetES.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromDataAssetWithWhereStatement))
    (list.toArray, offset + limit)
  }

  def readDataAssetsWhereMdpsIn(mdps: List[String]): Array[DataAssetES] = {
    if (mdps.isEmpty) Array() else {
      // TODO This query has a problem with java-scala array conversion
//      val selectFromDataAssetWithIdsInStatement: PreparedStatement = prepareStatement(s"SELECT id,name,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM $schema.$dataAssetTable WHERE id IN(?)")
//      val pgIds = connection.createArrayOf("int4", ids.toList.asJava.toArray)
//      selectFromDataAssetWithIdsInStatement.setArray(1, pgIds)

      // Alternative Option
      val repl:  String = mdps.map( mdp => "?") match {
        case q: List[String] => q.mkString(",")
      }
      val selectFromDataAssetWithIdsInStatement: PreparedStatement = prepareStatement(s"SELECT id,name,alias,description,metadata_path,type,subtype,tenant,properties,active,discovered_at,modified_at FROM $schema.$dataAssetTable WHERE metadata_path IN({{mdps}})".replace("{{mdps}}",repl))
      var index: Int = 0
      mdps.foreach(mdp => {
        index+=1
        selectFromDataAssetWithIdsInStatement.setString(index, mdp)
      })

      DataAssetES.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromDataAssetWithIdsInStatement)).toArray
    }
  }

  def readBusinessTermsWhereIdsIn(ids: List[Int]): Array[DataAssetES] = {
    if (ids.isEmpty) Array() else {
      // Alternative Option without list object
      val repl:  String = ids.map( id => "?") match {
        case q: List[String] => q.mkString(",")
      }
      val selectFromBusinessTermWithIdsInStatement: PreparedStatement = prepareStatement(additionalBusiness.getBTPartialIndexationSubquery2(schema, businessAssetsTable, businessAssetsTypeTable).replace("{{ids}}",repl))
      var index: Int = 0
      ids.foreach(id => {
        index+=1
        selectFromBusinessTermWithIdsInStatement.setInt(index, id)
      })

      DataAssetES.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromBusinessTermWithIdsInStatement)).toArray
    }
  }

  private case class Result (metadataPath: String, baId: Int, timestamp: Timestamp, literal: Short)

  def readUpdatedDataAssetsIdsSince(state: PostgresPartialIndexationReadState): (List[String], List[Int], PostgresPartialIndexationReadState) = {
    val unionSelectUpdatedStatement: PreparedStatement =
        prepareStatement(s"(" +
                                 s" SELECT metadata_path, 0, modified_at,1 FROM $schema.$dataAssetTable WHERE modified_at > ? " +
                                   "UNION " +
                                 s"SELECT metadata_path, 0, modified_at,2 FROM $schema.$keyDataAssetTable WHERE modified_at > ? " +
                                   "UNION " +
                                 s"SELECT key_data_asset.metadata_path, 0, key.modified_at,3 FROM $schema.$keyDataAssetTable AS key_data_asset, " +
                                    s"$schema.$keyTable AS key WHERE key.modified_at > ? " +
                                   "UNION " +
                                 s"SELECT metadata_path, 0, modified_at,4 FROM $schema.$businessAssetsDataAssetsTable WHERE modified_at > ? " +
                                   "UNION " +
                                 s"SELECT bus_assets_data_assets.metadata_path, 0, bus_assets.modified_at,5 FROM $schema.$businessAssetsDataAssetsTable AS bus_assets_data_assets, " +
                                    s"$schema.$businessAssetsTable AS bus_assets WHERE bus_assets_data_assets.business_assets_id = bus_assets.id and bus_assets.modified_at > ? " +
                                   "UNION " +
                                 additionalBusiness.getBTPartialIndexationSubquery1(schema, businessAssetsTable, businessAssetsTypeTable) +
                                s")")
    unionSelectUpdatedStatement.setTimestamp(1,state.readDataAsset)
    unionSelectUpdatedStatement.setTimestamp(2,state.readKeyDataAsset)
    unionSelectUpdatedStatement.setTimestamp(3,state.readKey)
    unionSelectUpdatedStatement.setTimestamp(4,state.readBusinessAssetsDataAsset)
    unionSelectUpdatedStatement.setTimestamp(5,state.readBusinessAssets)

    // Additional queries conditions for business Terms
    unionSelectUpdatedStatement.setInt(6, 6)
    unionSelectUpdatedStatement.setTimestamp(7,state.readBusinessAssets)

    val resultSet: ResultSet = executeQueryPreparedStatement(unionSelectUpdatedStatement)
    var list : List[Result] = List()
    while (resultSet.next()) {
      list = Result(resultSet.getString(1), resultSet.getInt(2), resultSet.getTimestamp(3), resultSet.getShort(4)) :: list
    }
    val mdps = list.filter(_.literal < 6).map(_.metadataPath).distinct
    val idsBusinessTerms = list.filter(_.literal == 6).map(_.baId)

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
    list.filter(_.literal == 6).map(_.timestamp).sortWith(_.getTime > _.getTime).headOption match {
      case Some(t) => state.readBusinessAssets = t
      case None =>
    }
    (mdps, idsBusinessTerms, state)
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


