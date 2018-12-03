package com.stratio.governance.agent.searcher.actors.extractor

import com.stratio.governance.agent.searcher.actors.dao.SourceDao

class DGExtractorParams(val sourceDao: SourceDao, val limit: Int, val periodMs : Long, val pauseMs: Long, val maxErrorRetry: Int, val delayMs: Long) {

  def createExponentialBackOff : ExponentialBackOff = ExponentialBackOff(pauseMs, pauseMs, maxErrorRetry, maxErrorRetry)
}
