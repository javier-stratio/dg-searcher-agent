package com.stratio.governance.agent.searcher.actors.extractor

import com.stratio.governance.agent.searcher.actors.extractor.dao.{ReaderElementDao, SourceDao}
import com.stratio.governance.agent.searcher.model.utils.ExponentialBackOff

case class DGExtractorParams(sourceDao: SourceDao, readerElementDao: ReaderElementDao, limit: Int, periodMs : Long, exponentialBackOff: ExponentialBackOff, delayMs: Long, managerName: String) {
}
