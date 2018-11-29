package com.stratio.governance.agent.searcher.model

case class KeyValuePair(id: Long,
                        key: String,
                        value: String,
                        updated_at: String
                        ) extends EntityRow(id)
