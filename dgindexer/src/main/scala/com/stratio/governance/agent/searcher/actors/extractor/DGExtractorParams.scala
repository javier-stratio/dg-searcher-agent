package com.stratio.governance.agent.searcher.actors.extractor

import com.stratio.governance.agent.searcher.actors.extractor.dao.SourceDao
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff

case class DGExtractorParams(sourceDao: SourceDao, limit: Int, periodMs : Long, exponentialBackOff: ExponentialBackOff, delayMs: Long) {
}
