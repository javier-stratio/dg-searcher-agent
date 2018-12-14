package com.stratio.governance.agent.searcher.actors.manager.scheduler.defimpl

import akka.actor.{ActorRef, ActorSystem}
import com.stratio.governance.agent.searcher.actors.manager.scheduler.Scheduler
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DGScheduler(system: ActorSystem, interval: Int, cronExpression: String) extends Scheduler {

  val scheduler: QuartzSchedulerExtension = QuartzSchedulerExtension(system)
  val TOTAL_INDEXER_REF: String =  "totalIndexerRef"

  def createTotalIndexerScheduling(): Unit = {
    scheduler.createSchedule(TOTAL_INDEXER_REF, Some("A cron expresion for total indexer"), cronExpression)
  }

  override def schedulePartialIndexation(actor: ActorRef, reference: String): Unit = {
    system.scheduler.schedule(interval seconds, interval seconds, actor, reference)
  }

  override def scheduleTotalIndexation(actor: ActorRef, reference: String): Unit = {
    scheduler.schedule(TOTAL_INDEXER_REF, actor, reference)
  }

}
