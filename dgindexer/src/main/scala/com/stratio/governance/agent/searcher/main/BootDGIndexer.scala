package com.stratio.governance.agent.searcher.main

import akka.actor.{ActorRef, ActorSystem, Props}
import com.stratio.governance.agent.searcher.actors.dao.postgres.PostgresSourceDao
import com.stratio.governance.agent.searcher.actors.dao.searcher.DGSearcherDao
import com.stratio.governance.agent.searcher.actors.manager.DGManager
import com.stratio.governance.agent.searcher.actors.extractor.{DGExtractor, DGExtractorParams}
import com.stratio.governance.agent.searcher.actors.indexer.{DGIndexer, DGIndexerParams}
import com.stratio.governance.agent.searcher.actors.manager.scheduler.defimpl.DGScheduler
import com.stratio.governance.agent.searcher.actors.manager.utils.defimpl.DGManagerUtils
import com.stratio.governance.agent.searcher.actors.utils.AdditionalBusiness
import com.stratio.governance.agent.searcher.http.defimpl.DGHttpManager
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent.Await
import scala.concurrent.duration._

object BootDGIndexer extends App with LazyLogging {

  logger.info("Initializing dg-indexer-agent ...")

  val service_name = "dg-indexer-agent"
  val manager_name = service_name + "_manager"
  val extractor_name = service_name + "_extractor"
  val indexer_name = service_name + "_indexer"

  val system = ActorSystem(service_name)

  var indexerRef: Option[ActorRef] = None
  var extractorRef: Option[ActorRef] = None
  var managerRef: Option[ActorRef] = None

  try {

    // Initialize indexer params objects
    val exponentialBackOff: ExponentialBackOff = ExponentialBackOff(AppConf.extractorExponentialbackoffPauseMs, AppConf.extractorExponentialbackoffMaxErrorRetry)
    val additionalBusiness: AdditionalBusiness = new AdditionalBusiness(AppConf.additionalBusinessDataAssetPrefix, AppConf.additionalBusinessBusinessAssetPrefix, AppConf.additionalBusinessBusinessAssetType, AppConf.additionalBusinessQualityRulePrefix, AppConf.additionalBusinessQualityRuleType, AppConf.additionalBusinessQualityRuleSubtype)
    val sourceDao = new PostgresSourceDao(AppConf.sourceConnectionUrl, AppConf.sourceConnectionUser, AppConf.sourceConnectionPassword, AppConf.sourceDatabase, AppConf.sourceSchema, AppConf.sourceConnectionInitialSize, AppConf.sourceConnectionMaxSize, exponentialBackOff, additionalBusiness)
    val httpManager = new DGHttpManager(AppConf.managerUrl, AppConf.indexerURL)
    val searcherDao = new DGSearcherDao(httpManager)
    val scheduler = new DGScheduler(system, AppConf.schedulerPartialEnabled, AppConf.schedulerPartialInterval, AppConf.schedulerTotalEnabled, AppConf.schedulerTotalCronExpresion)
    scheduler.createTotalIndexerScheduling()

    val dgManagerParams: DGManagerUtils = new DGManagerUtils(scheduler, sourceDao, List[Int](AppConf.managerRelevanceAlias, AppConf.managerRelevanceName, AppConf.managerRelevanceDescription, AppConf.managerRelevanceBusinessterm, AppConf.managerRelevanceQualityRules, AppConf.managerRelevanceKey, AppConf.managerRelevanceValue).map(i => i.toString))
    val dgExtractorParams: DGExtractorParams = DGExtractorParams(sourceDao, sourceDao, AppConf.extractorLimit, AppConf.extractorPeriodMs, exponentialBackOff, AppConf.extractorDelayMs, manager_name)
    val dgIndexerParams: DGIndexerParams = new DGIndexerParams(sourceDao, searcherDao, AppConf.indexerPartition, additionalBusiness)

    // initialize the actor system
    indexerRef = Some(system.actorOf(Props(classOf[DGIndexer], dgIndexerParams), indexer_name))
    extractorRef = Some(system.actorOf(Props(classOf[DGExtractor], indexerRef.get, dgExtractorParams), extractor_name))
    managerRef = Some(system.actorOf(Props(classOf[DGManager], extractorRef.get, dgManagerParams, searcherDao), manager_name))

    logger.info("dg-indexer-agent initiated!")

  } catch {
    case e: Throwable =>
      e.printStackTrace()
      logger.error("dg-indexer-agent Initializarion aborted!!!", e)
      System.exit(-1)
  }

  def cleanup() {
    if (indexerRef.isDefined) system.stop(indexerRef.get)
    if (extractorRef.isDefined) system.stop(extractorRef.get)
    if (managerRef.isDefined) system.stop(managerRef.get)
    Await.ready(system.terminate(), 1 minute)
    logger.info("DG-Searcher-Agent Stopped!")
  }

  Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      logger.info("Stopping DG-Searcher-Agent...")
      cleanup()
    }
  }))

}
