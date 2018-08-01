package com.stratio.governance.agent.searcher.model.es

import com.stratio.governance.agent.searcher.model.utils.GenerateESObject
import com.stratio.governance.agent.searcher.model.{DatabaseSchema, KeyValuePair}

case class DatabaseSchemaES(generated_id: String,
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

object DatabaseSchemaES {

  def fromDatabaseSchema(databaseSchema: DatabaseSchema, keyValuePair: KeyValuePair): DatabaseSchemaES = {
    DatabaseSchemaES(
      databaseSchema.metadata_path,
      GenerateESObject.genGenerated(databaseSchema, keyValuePair),
      databaseSchema.id,
      databaseSchema.name,
      "PostgreSQL",
      null,
      databaseSchema.agent_version,
      databaseSchema.server_version,
      databaseSchema.operation_command_type,
      databaseSchema.created_at,
      databaseSchema.updated_at,
      databaseSchema.metadata_path,
      databaseSchema.modification_time.getOrElse(null) match {
        case null => null
        case _    => databaseSchema.modification_time.get.toString
      },
      databaseSchema.access_time.getOrElse(null) match {
        case null => null
        case _    => databaseSchema.access_time.get.toString
      }
    )
  }

  def fromDatabaseSchemaList(databaseSchemaList: List[(DatabaseSchema, KeyValuePair)]): DatabaseSchemaES = {
    val databaseSchema = databaseSchemaList.head._1
    DatabaseSchemaES(
      GenerateESObject.genGeneratedId("PostgreSQL", databaseSchema.id),
      GenerateESObject.genGenerated(databaseSchema, databaseSchemaList),
      databaseSchema.id,
      databaseSchema.name,
      "PostgreSQL",
      null,
      databaseSchema.agent_version,
      databaseSchema.server_version,
      databaseSchema.operation_command_type,
      databaseSchema.created_at,
      databaseSchema.updated_at,
      databaseSchema.metadata_path,
      databaseSchema.modification_time.getOrElse(null) match {
        case null => null
        case 0    => null
        case _    => databaseSchema.modification_time.get.toString
      },
      databaseSchema.access_time.getOrElse(null) match {
        case null => null
        case 0    => null
        case _    => databaseSchema.access_time.get.toString
      }
    )
  }
}
