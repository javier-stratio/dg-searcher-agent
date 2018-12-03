package com.stratio.governance.agent.searcher.main

import com.stratio.governance.agent.searcher.actors.SearcherActorSystem
import com.stratio.governance.agent.searcher.actors.dao.PostgresSourceDao
import com.stratio.governance.agent.searcher.actors.extractor.{DGExtractor, DGExtractorParams}
import com.stratio.governance.agent.searcher.actors.indexer.{DGIndexer, DGIndexerParams}
import com.stratio.governance.agent.searcher.actors.indexer.dao.CustomSearcherDao

object BootDGIndexer extends App {

  // Initialize indexer params objects
  val sourceDao= new PostgresSourceDao(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, AppConf.sourceDatabase, AppConf.sourceConnectionInitialSize, AppConf.sourceConnectionMaxSize)
  val searcherDao= new CustomSearcherDao()
  val dgIndexerParams: DGIndexerParams = new DGIndexerParams(sourceDao, searcherDao)
  val dgExtractorParams: DGExtractorParams = new DGExtractorParams(sourceDao, AppConf.extractorLimit, AppConf.extractorPeriodMs, AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry, AppConf.extractorDelayMs)

  // initialize the actor system
  val actorSystem: SearcherActorSystem[DGExtractor, DGIndexer] = new SearcherActorSystem[DGExtractor, DGIndexer]("dgIndexer", classOf[DGExtractor], classOf[DGIndexer], dgExtractorParams, dgIndexerParams)
  actorSystem.initPartialIndexation()
}
