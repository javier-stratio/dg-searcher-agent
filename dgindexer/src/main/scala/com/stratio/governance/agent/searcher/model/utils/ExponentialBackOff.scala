package com.stratio.governance.agent.searcher.model.utils

case class ExponentialBackOff(initialPause: Long, actualPause: Long, initialNumExecToRestart: Int, actualNumExecToRestart: Int) {


  //TODO refactor this to allow a num max of retries, it should fail sometimes
  def next: ExponentialBackOff = actualNumExecToRestart match {
    case 0 =>
      ExponentialBackOff(initialPause, initialNumExecToRestart)
    case _ =>
      ExponentialBackOff(initialPause, actualPause*2, initialNumExecToRestart, actualNumExecToRestart-1)
  }

  def getPause: Long = actualPause
}

object ExponentialBackOff {

  def apply(initialPause: Long, initialNumExecToRestart: Int): ExponentialBackOff = apply(initialPause, initialPause, initialNumExecToRestart, initialNumExecToRestart)
}
