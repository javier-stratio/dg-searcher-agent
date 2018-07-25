package com.stratio.governance.agent.searcher.model

case class DatastoreEngine(id: Int,
                           `type`: String,
                           credentials: Option[String],
                           agent_version: String,
                           server_version: String,
                           created_at: String,
                           updated_at: String,
                           metadata_path: String,
                           name: String,
                           operation_command_type: String,
                           modification_time: Option[Long],
                           access_time: Option[Long]
                          ) extends EntityRow

object DatastoreEngine{
  val entity = "datastore"
}
