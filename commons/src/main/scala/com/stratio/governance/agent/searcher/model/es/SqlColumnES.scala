package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model.{FileColumn, KeyValuePair, SqlColumn}
import com.stratio.governance.agent.searcher.model.utils.GenerateESObject

case class SqlColumnES(generated_id: String,
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

object SqlColumnES {

  def fromSqlColumnList(sqlColumnList: List[(SqlColumn, KeyValuePair)]): SqlColumnES = {
    val sqlColumn = sqlColumnList.head._1
    SqlColumnES(sqlColumn.metadata_path,
      GenerateESObject.genGenerated(sqlColumn, sqlColumnList),
      sqlColumn.id,
      sqlColumn.name,
      "PostgreSQL",
      null,
      sqlColumn.agent_version,
      sqlColumn.server_version,
      sqlColumn.operation_command_type,
      sqlColumn.created_at,
      sqlColumn.updated_at,
      sqlColumn.metadata_path,
      sqlColumn.modification_time.getOrElse(null) match {
        case null => null
        case 0 => null
        case _ => sqlColumn.modification_time.get.toString
      }
      ,
      sqlColumn.access_time.getOrElse(null) match {
        case null => null
        case 0 => null
        case _ => sqlColumn.access_time.get.toString
      }
    )
  }

}
