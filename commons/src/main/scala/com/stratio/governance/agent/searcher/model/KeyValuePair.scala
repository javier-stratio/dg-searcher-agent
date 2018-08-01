package com.stratio.governance.agent.searcher.model

case class KeyValuePair(id: Int,
                        parent_id: Int,
                        parent_type: Int,
                        key_id: Int,
                        key: String,
                        value: String,
                        metadata_path: String,
                        agent_version: String,
                        server_version: String,
                        operation_command_type: String,
                        created_at: String,
                        updated_at: String,
                        modification_time: Option[Long],
                        access_time: Option[Long])
    extends EntityRow
