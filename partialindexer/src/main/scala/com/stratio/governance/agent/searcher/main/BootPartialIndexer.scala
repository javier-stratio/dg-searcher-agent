package com.stratio.governance.agent.searcher.main

import akka.actor.{ActorSystem, Props}
import com.stratio.governance.agent.searcher.actor.{MetadataPartialExtractor, PartialIndexer}
import org.apache.commons.dbcp.PoolingDataSource
import scalikejdbc._

object BootPartialIndexer extends App with AppConf {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000)
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/hakama", "postgres", "######", ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000))
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource].getConnection().setAutoCommit(false)



  // initialize the actor system
  val system = ActorSystem("ActorSystem")
  val metadataExtractor = system.actorOf(Props(classOf[MetadataPartialExtractor], partialIndexer, circuitBreakerConf), "extractor")
  private lazy val partialIndexer = system.actorOf(Props(classOf[PartialIndexer]), "indexer")

}
