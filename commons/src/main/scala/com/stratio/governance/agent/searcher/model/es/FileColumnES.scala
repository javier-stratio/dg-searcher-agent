package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model.{FileColumn, FileTable, KeyValuePair}
import com.stratio.governance.agent.searcher.model.utils.GenerateESObject

case class FileColumnES(generated_id: String,
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

object FileColumnES {

  def fromFileColumnList(fileColumnList: List[(FileColumn, KeyValuePair)]): FileColumnES = {
    val fileColumn = fileColumnList.head._1
    FileColumnES(
      fileColumn.metadata_path,
      GenerateESObject.genGeneratedFromList(fileColumn, fileColumnList),
      fileColumn.id,
      fileColumn.name,
      "HDFS",
      null,
      fileColumn.agent_version,
      fileColumn.server_version,
      fileColumn.operation_command_type,
      fileColumn.created_at,
      fileColumn.updated_at,
      fileColumn.metadata_path,
      fileColumn.modification_time.getOrElse(null) match {
        case null => null
        case 0    => null
        case _    => fileColumn.modification_time.get.toString
      },
      fileColumn.access_time.getOrElse(null) match {
        case null => null
        case 0    => null
        case _    => fileColumn.access_time.get.toString
      }
    )
  }

}
