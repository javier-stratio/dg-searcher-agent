package com.stratio.governance.agent.searcher.actors.manager.scheduler

import akka.actor.ActorRef

trait Scheduler {

  def schedulePartialIndexation(actor: ActorRef, referente: String)

  def scheduleTotalIndexation(actor: ActorRef, referente: String)

}
