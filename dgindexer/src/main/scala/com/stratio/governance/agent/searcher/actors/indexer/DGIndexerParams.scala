package com.stratio.governance.agent.searcher.actors.indexer

import java.sql.{Connection, ResultSet}

import akka.util.Timeout
import com.stratio.governance.agent.searcher.actors.indexer.dao.{SearcherDao, SourceDao}
import com.stratio.governance.agent.searcher.http.HttpRequester
import com.stratio.governance.agent.searcher.model._
import com.stratio.governance.agent.searcher.model.es.EntityRowES
//import com.stratio.governance.agent.searcher.model.utils.KeyValuePairMapping
import org.json4s.DefaultFormats
import scalikejdbc.ConnectionPool

import scala.concurrent.duration.MILLISECONDS

class DGIndexerParams extends IndexerParams {

  val sourceDbo: SourceDao = new SourceDao {

    val connection: Connection = ConnectionPool.borrow()
    implicit val formats: DefaultFormats.type = DefaultFormats
    implicit val timeout: Timeout =
      Timeout(60000, MILLISECONDS)

    override def keyValuePairProcess(ids: Array[Int]): List[EntityRow] = {

      // TODO POSTGRES INTEGRATION

//      val parentId = keyValuePair.parent_id
//      val parentType = keyValuePair.parent_type
//      val parentTable = KeyValuePairMapping.parentTable(parentType)
//
//      val statement = connection.createStatement
//      val resultSet: ResultSet = statement.executeQuery(s"select * from dg_metadata.$parentTable as parent " +
//        s"join dg_metadata.key_value_pair as kvp on parent.id = kvp.parent_id and parent_type = $parentType " +
//        s"where parent.id = $parentId")
//      val entity: Seq[EntityRowES] = KeyValuePairMapping.entityFromResultSet(parentType, resultSet)
//      resultSet.close
//      statement.close
//
//      entity.head
//      return null
      return null

    }

    override def businessTerms(ids: Array[Int]): List[EntityRow] = {
      // TODO POSTGRES INTEGRATION
      return null

    }
  }

  val searcherDbo: SearcherDao = new SearcherDao {
    override def index(doc: String): Unit = {
      HttpRequester.partialPostRequest(doc)
    }
  }

  override def getSourceDao(): SourceDao = {
    return sourceDbo
  }

  override def getSearcherDao(): SearcherDao = {
    return searcherDbo
  }

  override def getPartiton(): Int = {
    // TODO CONFIG
    return 10
  }
}
