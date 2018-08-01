package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model.utils.GenerateESObject
import com.stratio.governance.agent.searcher.model.{FileTable, KeyValuePair}

case class FileTableES(generated_id: String,
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
                       accessTime: String)
    extends EntityRowES

object FileTableES {

  def fromFileTableList(fileTableList: List[(FileTable, KeyValuePair)]): FileTableES = {
    val fileTable = fileTableList.head._1
    FileTableES(
      GenerateESObject.genGeneratedId(s"HDFS/${fileTable.datastore_engine_id}/FileTable", fileTable.id),
      GenerateESObject.genGenerated(fileTable, fileTableList),
      fileTable.id,
      fileTable.name,
      "PostgreSQL",
      null,
      fileTable.agent_version,
      fileTable.server_version,
      fileTable.operation_command_type,
      fileTable.created_at,
      fileTable.updated_at,
      fileTable.metadata_path,
      fileTable.modification_time.getOrElse(null) match {
        case null => null
        case 0    => null
        case _    => fileTable.modification_time.get.toString
      },
      fileTable.access_time.getOrElse(null) match {
        case null => null
        case 0    => null
        case _    => fileTable.access_time.get.toString
      }
    )
  }

}
