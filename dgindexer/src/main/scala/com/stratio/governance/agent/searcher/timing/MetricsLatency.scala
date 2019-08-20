package com.stratio.governance.agent.searcher.timing

import org.slf4j.{Logger, LoggerFactory}

class MetricsLatency(subject: String, detail: Option[String], loggable: Boolean) {

  private lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val iniTime: Long = System.currentTimeMillis()

  def observe: Long = {
    val endTime: Long = System.currentTimeMillis()
    val detail_string: String = detail match {
      case None => ""
      case Some(d) => " (" + d + ")"
    }
    val result: Long= (endTime - iniTime)
    if (loggable)
      logger.debug("time for " + subject + ": " + result + " ms" + detail_string )
    result
  }

}

object MetricsLatency {
  def build(subject: String, loggable: Boolean): MetricsLatency = {
    new MetricsLatency(subject, None, loggable)
  }
  def build(subject: String, detail: String, loggable: Boolean): MetricsLatency = {
    new MetricsLatency(subject, Some(detail), loggable)
  }
  def build(subject: String): MetricsLatency = {
    new MetricsLatency(subject, None, false)
  }
  def build(subject: String, detail: String): MetricsLatency = {
    new MetricsLatency(subject, Some(detail), false)
  }
}
