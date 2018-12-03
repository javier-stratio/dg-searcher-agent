package com.stratio.governance.agent.searcher.actors

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractor.{PartialIndexationMessage, TotalIndexationMessage}
import com.stratio.governance.agent.searcher.actors.extractor.DGExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.IndexerParams

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class SearcherActorSystem[A <: Actor,B <: Actor](name: String, extractor: Class[A], indexer: Class[B], extractorParams: DGExtractorParams, indexerParams: IndexerParams) {

  val system = ActorSystem(name)

  val indexerRef: ActorRef = system.actorOf(Props(indexer, indexerParams), name + "_indexer")
  val extractorRef: ActorRef = system.actorOf(Props(extractor, indexerRef, extractorParams), name + "_extractor")

  def performTotalIndexation(): Unit = {
    system.scheduler.scheduleOnce(extractorParams.delayMs millis, extractorRef,  TotalIndexationMessage(None, extractorParams.limit, extractorParams.createExponentialBackOff))
  }

  def initPartialIndexation(): Unit = {
    system.scheduler.scheduleOnce(extractorParams.delayMs millis, extractorRef, PartialIndexationMessage(None, extractorParams.limit, extractorParams.createExponentialBackOff))
  }

  def stopAll() : Unit = {
    system.stop(indexerRef)
    system.stop(extractorRef)
  }
}