package com.stratio.governance.agent.searcher.main

import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresSourceDao
import com.stratio.governance.agent.searcher.actors.dao.searcher.DGSearcherDao
import com.stratio.governance.agent.searcher.actors.extractor.{DGExtractor, DGExtractorParams}
import com.stratio.governance.agent.searcher.actors.indexer.{DGIndexer, DGIndexerParams}
import com.stratio.governance.agent.searcher.http.defimpl.DGHttpManager
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff


object BootDGIndexer extends App {

  // Initialize indexer params objects
  val exponentialBackOff: ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
  val sourceDao= new PostgresSourceDao(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, AppConf.sourceDatabase, AppConf.sourceSchema, AppConf.sourceConnectionInitialSize, AppConf.sourceConnectionMaxSize, exponentialBackOff)
  val httpManager = new DGHttpManager(AppConf.managerUrl, AppConf.indexerURL)
  val searcherDao = new DGSearcherDao(httpManager)
  val dgIndexerParams: DGIndexerParams = new DGIndexerParams(sourceDao, searcherDao, AppConf.indexerPartition)
  val dgExtractorParams: DGExtractorParams = DGExtractorParams(sourceDao, AppConf.extractorLimit, AppConf.extractorPeriodMs, exponentialBackOff, AppConf.extractorDelayMs)

  // initialize the actor system
  val actorSystem: SearcherActorSystem[DGExtractor, DGIndexer] = new SearcherActorSystem[DGExtractor, DGIndexer]("dgIndexer", classOf[DGExtractor], classOf[DGIndexer], dgExtractorParams, dgIndexerParams)
  actorSystem.initPartialIndexation()
}
