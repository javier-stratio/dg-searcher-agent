package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model.utils.GenerateESObject
import com.stratio.governance.agent.searcher.model.{KeyValuePair, SqlTable}

case class SqlTableES(generated_id: String,
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

object SqlTableES {

  def fromSqlTableList(sqlTableList: List[(SqlTable, KeyValuePair)]): SqlTableES = {
    val sqlTable = sqlTableList.head._1
    SqlTableES(sqlTable.metadata_path,
      GenerateESObject.genGenerated(sqlTable, sqlTableList),
      sqlTable.id,
      sqlTable.name,
      "PostgreSQL",
      null,
      sqlTable.agent_version,
      sqlTable.server_version,
      sqlTable.operation_command_type,
      sqlTable.created_at,
      sqlTable.updated_at,
      sqlTable.metadata_path,
      sqlTable.modification_time.getOrElse(null) match {
        case null => null
        case 0 => null
        case _ => sqlTable.modification_time.get.toString
      }
      ,
      sqlTable.access_time.getOrElse(null) match {
        case null => null
        case 0 => null
        case _ => sqlTable.access_time.get.toString
      }
    )
  }

}