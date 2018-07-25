package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model.DatastoreEngine
import com.stratio.governance.agent.searcher.model.utils.GenerateESObject

case class DatastoreEngineES(generated_id: String,
                             generated: GeneratedES,
                             id: Int,
                             name: String,
                            `type`: String,
                             credentials: String,
                             agentVersion: String,
                             serverVersion: String,
                             operationCommandType: String,
                             createdAt: String,
                             updatedAt: String,
                             metadataPath: String,
                             modificationTime: String,
                             accessTime: String) extends EntityRowES

object DatastoreEngineES {

  def fromDatastoreEngine(datastoreEngine: DatastoreEngine): DatastoreEngineES ={
    DatastoreEngineES(datastoreEngine.metadata_path,
      GenerateESObject.genGenerated(datastoreEngine), datastoreEngine.id,
      datastoreEngine.name,
      datastoreEngine.`type`,
      datastoreEngine.credentials.orNull,
      datastoreEngine.agent_version,
      datastoreEngine.server_version,
      datastoreEngine.operation_command_type,
      datastoreEngine.created_at,
      datastoreEngine.updated_at,
      datastoreEngine.metadata_path,
      datastoreEngine.modification_time.getOrElse(null) match {
        case null => null
        case _ => datastoreEngine.modification_time.get.toString
      }
    ,
      datastoreEngine.access_time.getOrElse(null) match {
        case null => null
        case _ => datastoreEngine.access_time.get.toString
      }
    )
  }
}