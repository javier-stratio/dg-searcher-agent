package com.stratio.governance.agent.searcher

import akka.actor.{Actor, ActorSystem, Props}
import com.stratio.governance.agent.searcher.actors.extractor.ExtractorParams
import com.stratio.governance.agent.searcher.actors.indexer.IndexerParams

class SearcherActorSystem[A <: Actor,B <: Actor](name: String, extractor: Class[A], indexer: Class[B], extractorParams: ExtractorParams, indexerParams: IndexerParams) {

  val system = ActorSystem(name)

  def initialize(): Unit = {

    val ind = system.actorOf(Props(indexer, indexerParams), name + "_indexer")
    val ext = system.actorOf(Props(extractor, ind, extractorParams), name + "_extractor")

  }

}