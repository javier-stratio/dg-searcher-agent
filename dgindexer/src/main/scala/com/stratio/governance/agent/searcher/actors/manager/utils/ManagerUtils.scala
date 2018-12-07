package com.stratio.governance.agent.searcher.actors.manager.utils

import com.stratio.governance.agent.searcher.actors.manager.scheduler.Scheduler

trait ManagerUtils {

  // Timing methods
  @throws(classOf[ManagerUtilsException])
  def getScheduler(): Scheduler

  // Manager Schema Operations
  @throws(classOf[ManagerUtilsException])
  def getGeneratedModel(): String

}

case class ManagerUtilsException(message: String) extends Throwable(message)