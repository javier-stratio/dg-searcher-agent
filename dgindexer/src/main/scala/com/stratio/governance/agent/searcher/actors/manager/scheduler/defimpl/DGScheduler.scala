package com.stratio.governance.agent.searcher.actors.manager.scheduler.defimpl

import akka.actor.{ActorRef, ActorSystem}
import com.stratio.governance.agent.searcher.actors.manager.scheduler.Scheduler
import com.typesafe.akka.extension.quartz.QuartzSchedulerExtension
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class DGScheduler(system: ActorSystem, partialEnabled: Boolean, interval: Int, totalEnabled: Boolean, cronExpression: String) extends Scheduler {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val scheduler: QuartzSchedulerExtension = QuartzSchedulerExtension(system)
  val TOTAL_INDEXER_REF: String =  "totalIndexerRef"

  def createTotalIndexerScheduling(): Unit = {
    if (totalEnabled) {
      LOG.info("creating " + TOTAL_INDEXER_REF + " scheduling with cron expresion: " + cronExpression)
      scheduler.createSchedule( TOTAL_INDEXER_REF, Some( "A cron expresion for total indexer" ), cronExpression )
    } else {
      LOG.info("total indexation scheduling disabled. " + TOTAL_INDEXER_REF + " has not been created.")
    }
  }

  override def schedulePartialIndexation(actor: ActorRef, reference: String): Unit = {
    if (partialEnabled) {
      LOG.info( "initiating partial indexation scheduling with interval: " + interval + " seconds" )
      system.scheduler.schedule( interval seconds, interval seconds, actor, reference )
    } else {
      LOG.info( "partial indexation scheduling disabled. scheduler has not been initiated." )
    }
  }

  override def scheduleTotalIndexation(actor: ActorRef, reference: String): Unit = {
    if (totalEnabled) {
      LOG.info( "initiating total indexation scheduling.")
      scheduler.schedule( TOTAL_INDEXER_REF, actor, reference )
    } else {
      LOG.info( "total indexation scheduling disabled. scheduler has not been initiated." )
    }
  }

}
