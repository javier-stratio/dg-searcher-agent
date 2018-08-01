package com.stratio.governance.agent.searcher.totalindex.main

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import com.stratio.governance.agent.searcher.totalindex.actor.{MetadataTotalExtractor, TotalIndexer}
import org.apache.commons.dbcp2.{PoolableConnection, PoolingDataSource}
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}
import com.stratio.governance.agent.searcher.http.HttpRequester._
import com.stratio.governance.agent.searcher.totalindex.main.TotalIndexer._

import scala.concurrent.ExecutionContextExecutor

object BootTotalIndexer extends App with AppConf {

  // initialize JDBC driver & connection pool
  Class.forName("org.postgresql.Driver")
  ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000)
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/hakama",
                           "postgres",
                           "######",
                           ConnectionPoolSettings.apply(initialSize = 1000, maxSize = 1000))
  ConnectionPool
    .dataSource()
    .asInstanceOf[PoolingDataSource[PoolableConnection]]
    .setAccessToUnderlyingConnectionAllowed(true)
  ConnectionPool.dataSource().asInstanceOf[PoolingDataSource[PoolableConnection]].getConnection().setAutoCommit(false)

  // initialize the actor system
  implicit val system: ActorSystem                = ActorSystem("ActorSystem")
  implicit val materializer: ActorMaterializer    = ActorMaterializer()
  implicit val executor: ExecutionContextExecutor = system.dispatcher

  //TODO for a total indexation, a model updating is needed. So we will need first to create a new governance domain manually.
  //TODO maybe, as part of the process, a new actor with that functionality could be included here.
//  private lazy val totalIndexer = system.actorOf(Props(classOf[TotalIndexer]), "indexer")
//  val metadataExtractor         = system.actorOf(Props(classOf[MetadataTotalExtractor], totalIndexer), "extractor")

  indexAll()

}
