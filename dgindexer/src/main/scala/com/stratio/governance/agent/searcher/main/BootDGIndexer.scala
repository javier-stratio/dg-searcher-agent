package com.stratio.governance.agent.searcher.main

import com.stratio.governance.agent.searcher.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.extractor.{ExtractorParams, MetadataDGExtractor}
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexerParams
import com.stratio.governance.agent.searcher.actors.indexer.DGIndexer
import org.apache.commons.dbcp.PoolingDataSource
import scalikejdbc._

object BootDGIndexer extends App with AppConf {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000)
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/hakama", "postgres", "######", ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(false)

  // Initialize indexer params objects
  val dgIndexerParams: DGIndexerParams = new DGIndexerParams()
  val dgExtractorParams: ExtractorParams = new ExtractorParams {}

  // initialize the actor system
  val actorSystem: SearcherActorSystem[MetadataDGExtractor, DGIndexer] = new SearcherActorSystem[MetadataDGExtractor, DGIndexer]("dgIndexer", classOf[MetadataDGExtractor], classOf[DGIndexer], dgExtractorParams, dgIndexerParams)
  actorSystem.initialize()

}
