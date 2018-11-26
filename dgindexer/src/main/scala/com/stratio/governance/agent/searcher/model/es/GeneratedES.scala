package com.stratio.governance.agent.searcher.model.es

case class GeneratedES(entity: String,
                       `type`: String,
                       id: String,
                       parent_id: String,
                       categories: Seq[Seq[CategoryES]],
                       keyValuePairs: KeyValuePairES) {

}
