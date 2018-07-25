package com.stratio.governance.agent.searcher.totalindex.main

import akka.actor.{ActorSystem, Props}
import com.stratio.governance.agent.searcher.totalindex.actor.{MetadataTotalExtractor, TotalIndexer}
import org.apache.commons.dbcp.PoolingDataSource
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object BootTotalIndexer extends App with AppConf {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000)
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/hakama", "postgres", "######", ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(false)

  // initialize the actor system
  val system = ActorSystem("ActorSystem")

  //TODO for a total indexation, a model updating is needed. So we will need first to create a new governance domain manually.
  //TODO maybe, as part of the process, a new actor with that functionality could be included here.
  val metadataExtractor = system.actorOf(Props(classOf[MetadataTotalExtractor], totalIndexer, circuitBreakerConf), "extractor")
  private lazy val totalIndexer = system.actorOf(Props(classOf[TotalIndexer]), "indexer")

}
