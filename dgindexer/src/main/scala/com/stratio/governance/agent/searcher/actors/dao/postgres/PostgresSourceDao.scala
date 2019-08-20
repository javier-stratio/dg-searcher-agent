package com.stratio.governance.agent.searcher.actors.dao.postgres

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp}
import java.time.LocalDateTime

import com.stratio.governance.agent.searcher.actors.extractor.dao.ReaderElementDao
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.domain.SearchElementDomain
import com.stratio.governance.agent.searcher.domain.SearchElementDomain.{BusinessAssetReaderElement, DataAssetReaderElement, QualityRuleReaderElement}
import com.stratio.governance.agent.searcher.model.QualityRule
import com.stratio.governance.agent.searcher.timing.MetricsLatency

//import collection.JavaConverters._

import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.extractor.dao.{SourceDao => ExtractorSourceDao}
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SourceDao => IndexerSourceDao}
import com.stratio.governance.agent.searcher.actors.manager.dao.{SourceDao => ManagerSourceDao}
import com.stratio.governance.agent.searcher.model.es.ElasticObject
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
                        loggable: Boolean,
                        allowedToCreateContext: Boolean = false) extends ExtractorSourceDao with ReaderElementDao with IndexerSourceDao with ManagerSourceDao {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  // initialize JDBC driver & connection pool
  val datasource: Datasource = Datasource.getDataSource(sourceConnectionUrl, "org.postgresql.Driver", sourceConnectionUser, sourceConnectionPassword, maxSize)
  private var connection: Connection = datasource.getConnection

  val initialExponentialBackOff: ExponentialBackOff = exponentialBackOff
  implicit val formats: DefaultFormats.type = DefaultFormats
  implicit val timeout: Timeout = Timeout(60000, MILLISECONDS)

  private val dataAssetTable: String = "data_asset"
  private val keyDataAssetTable: String = "key_data_asset"
  private val keyBusinessAssetTable: String = "key_business_assets"
  private val keyQualityTable: String = "key_quality"
  private val keyTable: String = "key"
  private val businessAssetsDataAssetsTable: String = "business_assets_data_asset"
  private val businessAssetsQualityTable: String = "business_assets_quality"
  private val businessAssetsTable: String = "business_assets"
  private val businessAssetsTypeTable: String = "business_assets_type"
  private val businessAssetsStatusTable: String = "bpm_status"

  private val qualityRulesTable: String = "quality"

  private var preparedStatements: Map[String, PreparedStatement] = Map[String, PreparedStatement]()

  private lazy val dataAssetReaderElement: DataAssetReaderElement = new DataAssetReaderElement(schema, dataAssetTable, keyDataAssetTable, keyTable, businessAssetsDataAssetsTable, businessAssetsTable, qualityRulesTable)
  private lazy val businessAssetReaderElement: BusinessAssetReaderElement = new BusinessAssetReaderElement(schema, businessAssetsTable, businessAssetsTypeTable, businessAssetsStatusTable, keyBusinessAssetTable, keyTable)
  private lazy val qualityRuleReaderElement: QualityRuleReaderElement = new QualityRuleReaderElement(schema, qualityRulesTable , keyQualityTable, keyTable, businessAssetsQualityTable, businessAssetsTable)

  def restartConnection(): Unit = {
    connection.commit()
    connection.close()
    connection = datasource.getConnection
    preparedStatements = Map[String, PreparedStatement]()
  }

  override def prepareStatement(query: String): PreparedStatement = {
    val latency: MetricsLatency = MetricsLatency.build( "prepareStatement", query, loggable)
    val prepStatement: PreparedStatement = preparedStatements.get( query ) match {
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
    latency.observe
    prepStatement
  }

  def executeQuery(sql: String): ResultSet = {
    val latency: MetricsLatency = MetricsLatency.build( "executeQuery", sql, loggable)
    try{
      val rs = connection.createStatement().executeQuery(sql)
      exponentialBackOff = initialExponentialBackOff
      rs
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executeQuery - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage, exception)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executeQuery(sql)
    } finally {
      latency.observe
    }
  }

  def execute(sql: String): Unit = {
    val latency: MetricsLatency = MetricsLatency.build( "execute", sql, loggable)
    try{
      val rs = connection.createStatement().execute(sql)
      exponentialBackOff = initialExponentialBackOff
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executeQuery - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage, exception)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        execute(sql)
    } finally {
      latency.observe
    }
  }

  def executePreparedStatement(sql: PreparedStatement): Unit = {
    val latency: MetricsLatency = MetricsLatency.build( "executePreparedStatement", loggable)
    try{
      val rs = sql.execute()
      exponentialBackOff = initialExponentialBackOff
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executePreparedStatement - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage, exception)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executePreparedStatement(sql)
    } finally {
      latency.observe
    }
  }

  def executeQueryPreparedStatement(sql: PreparedStatement): ResultSet = {
    val latency: MetricsLatency = MetricsLatency.build( "executeQueryPreparedStatement", loggable)
    try{
      val rs = sql.executeQuery()
      exponentialBackOff = initialExponentialBackOff
      rs
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executePreparedStatement - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage, exception)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executeQueryPreparedStatement(sql)
    } finally {
      latency.observe
    }
  }


  def executeUpdatePreparedStatement(sql: PreparedStatement): Unit = {
    val latency: MetricsLatency = MetricsLatency.build( "executeUpdatePreparedStatement", loggable)
    try{
      val rs = sql.executeUpdate()
      exponentialBackOff = initialExponentialBackOff
    } catch {
      case exception: SQLException =>
        // See https://www.postgresql.org/docs/current/errcodes-appendix.html
        LOG.error("executePreparedStatement - Exponential BackOff in progress ... . " + sql + ", " + exception.getMessage, exception)
        Thread.sleep(exponentialBackOff.actualPause)
        if (exception.getSQLState.startsWith("08")) { // Problems with Connection
          restartConnection()
        }
        exponentialBackOff = exponentialBackOff.next
        executeUpdatePreparedStatement(sql)
    } finally {
      latency.observe
    }
  }

  private var status : Option[PostgresPartialIndexationReadState] = None

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

  override def keyValuePairForDataAsset(mdps: List[String]): List[KeyValuePair] = {
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

        KeyValuePair.getValueFromResult( executeQueryPreparedStatement(selectKeyValuePairStatement) )
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

  override def businessTermsForDataAsset(mdps: List[String]): List[BusinessAsset] = {
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

  def readElementsSince(offset: Int, limit: Int): (Array[ElasticObject], Int) = {
    val dataAssetQuery = getTotalIndexationQueryInfo(dataAssetReaderElement, "", "")
    val businessAssetQuery = getTotalIndexationQueryInfo(businessAssetReaderElement, additionalBusiness.btType, "")
    val qualityRuleQuery = getTotalIndexationQueryInfo(qualityRuleReaderElement, additionalBusiness.qrType, additionalBusiness.qrSubtype)
    val selectFromDataAssetWithWhereStatement: PreparedStatement = prepareStatement("(" +
      dataAssetQuery + " UNION " +
      businessAssetQuery + " UNION " +
      qualityRuleQuery + ") order by id asc, name asc limit ? offset ?")
    selectFromDataAssetWithWhereStatement.setInt(1, limit)
    selectFromDataAssetWithWhereStatement.setInt(2, offset)
    val list = ElasticObject.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromDataAssetWithWhereStatement))
    (list.toArray, offset + limit)
  }

  def readDataAssetsWhereMdpsIn(mdps: List[String]): Array[ElasticObject] = {
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

      ElasticObject.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromDataAssetWithIdsInStatement)).toArray
    }
  }

  def readBusinessAssetsWhereIdsIn(ids: List[Int]): Array[ElasticObject] = {
    if (ids.isEmpty) Array() else {
      // Alternative Option without list object
      val repl:  String = ids.map( id => "?") match {
        case q: List[String] => q.mkString(",")
      }
      val selectFromBusinessAssetWithIdsInStatement: PreparedStatement = prepareStatement(additionalBusiness.getBusinessAssetsPartialIndexationSubqueryInfoById(schema, businessAssetsTable, businessAssetsTypeTable, businessAssetsStatusTable).replace("{{ids}}",repl))
      var index: Int = 0
      ids.foreach(id => {
        index+=1
        selectFromBusinessAssetWithIdsInStatement.setInt(index, id)
      })

      ElasticObject.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromBusinessAssetWithIdsInStatement)).toArray
    }
  }

  override def readQualityRulesWhereIdsIn(ids: List[Int]): Array[ElasticObject] = {
    if (ids.isEmpty) Array() else {
      // Alternative Option without list object
      val repl:  String = ids.map( id => "?") match {
        case q: List[String] => q.mkString(",")
      }
      val selectFromBusinessTermWithIdsInStatement: PreparedStatement = prepareStatement(additionalBusiness.getQualityRulesPartialIndexationSubqueryInfoById(schema, qualityRulesTable).replace("{{ids}}",repl))
      var index: Int = 0
      ids.foreach(id => {
        index+=1
        selectFromBusinessTermWithIdsInStatement.setInt(index, id)
      })

      ElasticObject.getValuesFromResult(additionalBusiness.adaptInfo, executeQueryPreparedStatement(selectFromBusinessTermWithIdsInStatement)).toArray
    }

  }

  private case class Result (metadataPath: String, baId: Int, timestamp: Timestamp, literal: Short)

  def readUpdatedElementsIdsSince(state: PostgresPartialIndexationReadState): (List[String], List[Int],List[Int], PostgresPartialIndexationReadState) = {

    val ref: Int = 1
    val dataAssetQueryInfo = getPartialIndexationQueryInfo(dataAssetReaderElement, ref)
    val businessAssetsQueryInfo = getPartialIndexationQueryInfo(businessAssetReaderElement, ref + dataAssetQueryInfo._2.size)
    val qualityRuleQueryInfo = getPartialIndexationQueryInfo(qualityRuleReaderElement, ref + dataAssetQueryInfo._2.size + businessAssetsQueryInfo._2.size)

    val unionSelectUpdatedStatement: PreparedStatement =
        prepareStatement(s"(" +
          dataAssetQueryInfo._1 +
           "UNION " +
          businessAssetsQueryInfo._1 +
           "UNION " +
          qualityRuleQueryInfo._1 +
          s")")

    // queries conditions
    val tsList :List[(Int, String)] = dataAssetQueryInfo._2 ++ businessAssetsQueryInfo._2 ++ qualityRuleQueryInfo._2

    tsList.foreach(field => {
      try {
        val ts: Timestamp = state.getTimeStamp( field._2 )
        unionSelectUpdatedStatement.setTimestamp( field._1, ts )
      } catch {
        case e: RuntimeException => LOG.warn("field " + field + " can not be processed while building query", e)
      }
    })

    val resultSet: ResultSet = executeQueryPreparedStatement(unionSelectUpdatedStatement)
    var list : List[Result] = List()
    while (resultSet.next()) {
      list = Result(resultSet.getString(1), resultSet.getInt(2), resultSet.getTimestamp(3), resultSet.getShort(4)) :: list
    }
    val mdps = list.filter(_.literal < (ref + dataAssetQueryInfo._2.size)).map(_.metadataPath).distinct
    val idsBusinessTerms = list.filter(e => (e.literal >= (ref + dataAssetQueryInfo._2.size) && e.literal < (ref + dataAssetQueryInfo._2.size + businessAssetsQueryInfo._2.size))).map(_.baId).distinct
    val idsQualityRules = list.filter(e => (e.literal >= (ref + dataAssetQueryInfo._2.size + businessAssetsQueryInfo._2.size)) && e.literal < (ref + dataAssetQueryInfo._2.size + businessAssetsQueryInfo._2.size + qualityRuleQueryInfo._2.size)).map(_.baId).distinct

    if (!list.isEmpty) {
      LOG.debug("Updating partial indexation timestamps")
      val updatedTimes: List[(String, Timestamp)] = tsList.map( field => {
        list.filter( _.literal == field._1 ).map( _.timestamp ).sortWith( _.getTime > _.getTime ).headOption match {
          case Some( t ) => (field._2, t)
          case None => (field._2, state.getTimeStamp(field._2))
        }
      } )

      val maxUpdatedTimesByField: Map[String, Timestamp] = updatedTimes.groupBy( ut => ut._1 ).mapValues( l => {
        l.map( _._2 ).sortWith( _.getTime > _.getTime ).headOption match {
          case Some( t ) => t
          case None => {
            LOG.warn( "field can not be processed while ordering times" )
            Timestamp.valueOf( LocalDateTime.MIN )
          }
        }
      } )

      // Update the fields Timestamps
      state.fields = state.fields.map( field => {
        val ts: Timestamp = maxUpdatedTimesByField.get( field._1 ) match {
          case Some( t ) => t
          case None => {
            LOG.warn( "field " + field + " can not be processed while setting timestamps" )
            Timestamp.valueOf( LocalDateTime.MIN )
          }
        }
        (field._1, (field._2._1, ts))
      } )
    } else {
      LOG.debug("No partial indexation timestamps to update\")")
    }

    (mdps, idsBusinessTerms, idsQualityRules, state)
  }

  override def getKeys(): List[String] = {
    PostgresSourceDao.getKeysFromResult(executeQueryPreparedStatement(prepareStatement(s"SELECT DISTINCT key FROM $schema.$keyTable")))
  }

  def readPartialIndexationState(): PostgresPartialIndexationReadState = status.getOrElse({
    status = Some(PostgresPartialIndexationReadState(this, schema))
    if (!status.get.isDataAssetMetadataTableCreated) status.get.createDataAssetMetadataTable()
    status.get.read(connection)
    status.get
  })

  def writePartialIndexationState(state: PostgresPartialIndexationReadState): Unit = {
    status = Some(state)
    status.get.update(connection)
  }

  override def qualityRulesForDataAsset(mdps: List[String]): List[QualityRule] = {
    if (!mdps.isEmpty) {
      try {
        // Alternative Option
        val repl: String = mdps.map( mdp => "?" ) match {
          case q: List[String] => q.mkString( "," )
        }
        val selectQualityRulesStatement: PreparedStatement = prepareStatement( s"SELECT metadata_path, name, modified_at " +
          s"FROM $schema.$qualityRulesTable " +
          s"WHERE metadata_path IN({{mdps}})".replace( "{{mdps}}", repl ) +
          "" )
        var index: Int = 0
        mdps.foreach( mdp => {
          index += 1
          selectQualityRulesStatement.setString( index, mdp )
        } )
        QualityRule.getValueFromResult( executeQueryPreparedStatement( selectQualityRulesStatement ) )
      } catch {
        case e: Throwable =>
          LOG.error( "error while getting Business Assets", e )
          List[QualityRule]()
      }
    } else {
      LOG.debug("There is no ids to get Quality Rules")
      List[QualityRule]()
    }
  }

  override def keyValuePairForBusinessAsset(ids: List[Long]): List[KeyValuePair] = {
    if (!ids.isEmpty) {
      try {
        val repl: String = ids.map( id => "?" ) match {
          case q: List[String] => q.mkString( "," )
        }
        val selectKeyValuePairStatement: PreparedStatement = prepareStatement( s"SELECT key_asset.business_assets_id, key.key, key_asset.value, key.modified_at, key_asset.modified_at FROM $schema.$keyBusinessAssetTable AS key_asset, $schema.$keyTable AS key WHERE key.id=key_asset.key_id AND key_asset.business_assets_id IN ({{ids}})".replace( "{{ids}}", repl ) )
        var index: Int = 0
        ids.foreach( id => {
          index += 1
          selectKeyValuePairStatement.setLong( index, id )
        } )

        KeyValuePair.getValueFromResult( executeQueryPreparedStatement(selectKeyValuePairStatement) )
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

  override def keyValuePairForQualityRule(ids: List[Long]): List[KeyValuePair] = {
    if (!ids.isEmpty) {
      try {
        val repl: String = ids.map( id => "?" ) match {
          case q: List[String] => q.mkString( "," )
        }
        val selectKeyValuePairStatement: PreparedStatement = prepareStatement( s"SELECT key_quality.quality_id, key.key, key_quality.value, key.modified_at, key_quality.modified_at FROM $schema.$keyQualityTable AS key_quality, $schema.$keyTable AS key WHERE key.id=key_quality.key_id AND key_quality.quality_id IN ({{ids}})".replace( "{{ids}}", repl ) )

        var index: Int = 0
        ids.foreach( id => {
          index += 1
          selectKeyValuePairStatement.setLong( index, id )
        } )

        KeyValuePair.getValueFromResult( executeQueryPreparedStatement(selectKeyValuePairStatement) )
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

  override def businessRulesForQualityRule(ids: List[Long]): List[BusinessAsset] = {
    if (!ids.isEmpty) {
      try {
        val repl: String = ids.map( id => "?" ) match {
          case q: List[String] => q.mkString( "," )
        }
        val selectBusinessTermsStatement: PreparedStatement = prepareStatement( s"SELECT bus_assets_quality.quality_id, bus_assets.name, bus_assets.description, bus_assets_status.name, " +
          s"bus_assets_type.name, bus_assets_quality.modified_at, bus_assets.modified_at " +
          s"FROM $schema.$businessAssetsQualityTable AS bus_assets_quality, " +
          s"$schema.$businessAssetsTable AS bus_assets, " +
          s"$schema.$businessAssetsTypeTable AS bus_assets_type, " +
          s"$schema.$businessAssetsStatusTable AS bus_assets_status " +
          s"WHERE bus_assets_quality.quality_id IN({{mdps}})".replace( "{{mdps}}", repl ) +
          "AND bus_assets.id=bus_assets_quality.business_assets_id " +
          "AND bus_assets_type.id=bus_assets.business_assets_type_id " +
          "AND bus_assets_status.id=bus_assets.business_assets_status_id" )
        var index: Int = 0
        ids.foreach( id => {
          index += 1
          selectBusinessTermsStatement.setLong( index, id )
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

  def getTotalIndexationQueryInfo[W](w: W, typ: String, subTpe: String)(implicit reader: SearchElementDomain.Reader[W]): String = {
    reader.getTotalIndexationQueryInfo(w, typ, subTpe)
  }

  def getPartialIndexationQueryInfo[W](w: W, resultNumber: Int)(implicit reader: SearchElementDomain.Reader[W]): (String, List[(Int, String)]) = {
    reader.getPartialIndexationQueryInfo(w, resultNumber)
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


