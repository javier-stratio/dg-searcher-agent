package com.stratio.governance.agent.searcher.actors.extractor

case class ExponentialBackOff(initialPause: Long, actualPause: Long, initialNumExecToRestart: Int, actualNumExecToRestart: Int) {

  def next: ExponentialBackOff = actualNumExecToRestart match {
    case 0 =>
      ExponentialBackOff(initialPause,initialPause, initialNumExecToRestart, initialNumExecToRestart)
    case _ =>
      ExponentialBackOff(initialPause, actualPause*2, initialNumExecToRestart, actualNumExecToRestart-1)
  }

  def getPause: Long = actualPause
}
