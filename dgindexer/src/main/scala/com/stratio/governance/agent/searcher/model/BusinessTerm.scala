package com.stratio.governance.agent.searcher.model

case class BusinessTerm(id: Long,
                        term: String,
                        updated_at: String
                   ) extends EntityRow(id)
