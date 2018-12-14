package com.stratio.governance.agent.searcher.actors.manager

import com.stratio.governance.agent.searcher.actors.dao.searcher.{DGSearcherDao, DGSearcherDaoException}
import com.stratio.governance.agent.searcher.actors.manager.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.manager.utils.defimpl.DGManagerUtils
import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import org.apache.http.client.methods.CloseableHttpResponse
import org.scalatest.FlatSpec

class HttpManagerMock extends HttpManager {

  @throws(classOf[HttpException])
  override def partialPostRequest(model: String, json: String): String = ???

  @throws(classOf[HttpException])
  override def getIndexerdomains(): String = {
    "{\"total_time_elapsed\":128,\"domains\":[{\"domain\":\"governance_search\",\"token\":null,\"status\":\"ENDED\",\"last_status_change\":\"2018-11-29T15:03:30.488+0000\"},{\"domain\":\"movies\",\"token\":\"3e33463a-2ed9-4c15-87c0-1a520e3c1cb2\",\"status\":\"INDEXING\",\"last_status_change\":\"2018-11-29T14:58:46.240+0000\"}]}"
  }

  @throws(classOf[HttpException])
  override def initTotalIndexationProcess(model: String): String = {
    model match {
      case "case1" => {
        "{\"token\":\"dde76989-4725-4ba2-82fd-400b07d32b32\",\"status\":\"INDEXING\",\"last_status_change\":\"2018-12-05T07:57:07.911+0000\",\"time_stats\":{\"total\":16068}}"
      }
      case "case2" => {
        "{\"token\":\"dde76989-4725-4ba2-82fd-400b07d32b32\",\"status\":\"CANCELLED\",\"last_status_change\":\"2018-12-05T07:57:07.911+0000\",\"time_stats\":{\"total\":16068}}"
      }
      case "_" => {
        "{}"
      }
    }
  }

  override def getManagerModels(): String = {
    "{\"total\":1,\"domains\":[{\"id\":\"governance_search\",\"name\":\"Governance Search V0.4\"}]}"
  }

  override def totalPostRequest(model: String, token: String, json: String): String = ???

  override def insertOrUpdateModel(model: String, json: String): Unit = ???

  override def finishTotalIndexationProcess(model: String, token: String): Unit = ???

  override def cancelTotalIndexationProcess(model: String, token: String): Unit = ???
}

class SourceDaoMock extends SourceDao {

  override def getKeys(): List[String] = {
    List("OWNER","QUALITY","FINANCIAL")
  }

}

class DGManagerJsonTest extends FlatSpec {

  val httpManager: HttpManager = new HttpManagerMock()
  val sourceDao: SourceDao = new SourceDaoMock()
  val managerUtils = new DGManagerUtils(null, sourceDao)
  val searchDao = new DGSearcherDao(httpManager)

  "First checkTotalIndexation " should " parse true case" in {
    val res = searchDao.checkTotalIndexation("movies")
    assertResult(true)(res._1)
    assertResult(Some("3e33463a-2ed9-4c15-87c0-1a520e3c1cb2"))(res._2)
  }

  "Second checkTotalIndexation " should " parse false case" in {
    val res = searchDao.checkTotalIndexation("governance_search")
    assertResult(false)(res._1)
    assertResult(None)(res._2)
  }

  "Third checkTotalIndexation " should " parse null case" in {
    val res = searchDao.checkTotalIndexation("otherthing")
    assertResult(false)(res._1)
    assertResult(None)(res._2)
  }

  "Right initTotalIndexation Process " should " parse token" in {
    val res = searchDao.initTotalIndexationProcess("case1")
    assertResult("dde76989-4725-4ba2-82fd-400b07d32b32")(res)
  }

  "Wrong initTotalIndexation Process " should " throws exception" in {
    try {
      searchDao.initTotalIndexationProcess("case2")
      assert(false,"method must throw an exception")
    } catch {
      case DGSearcherDaoException(message) => {
        assertResult("status is not INDEXING")(message)
      }
    }
  }

  "Right getModels Process " should " parse the list" in {
    val res = searchDao.getModels()
    assertResult(1)(res.length)
    assertResult("governance_search")(res.head)
  }

  "Model Json File " should " match to key list retrieved" in {
    val res: String = managerUtils.getGeneratedModel()
    assertResult("{  \"name\": \"Governance Search\",  \"model\": {    \"id\": \"id\",    \"language\": \"spanish\",    \"fields\": [      {        \"field\": \"id\",        \"name\": \"Name\",        \"type\": \"text\",        \"searchable\": false,        \"sortable\": false,        \"aggregable\": false      },      {        \"field\": \"name\",        \"name\": \"Name\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"description\",        \"name\": \"Description\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"metadataPath\",        \"name\": \"Metadata Path\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"type\",        \"name\": \"Data Stores\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"subtype\",        \"name\": \"Type\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"tenant\",        \"name\": \"Tenant\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"active\",        \"name\": \"Active\",        \"type\": \"boolean\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"discoveredAt\",        \"name\": \"Access Time\",        \"type\": \"date\",        \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"modifiedAt\",        \"name\": \"Access Time\",        \"type\": \"date\",        \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"dataStore\",        \"name\": \"Data Store\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"businessTerms\",        \"name\": \"Business Terms\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"keys\",        \"name\": \"Keys\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }                     ,{        \"field\": \"key.OWNER\",        \"name\": \"Key OWNER\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }                     ,{        \"field\": \"key.QUALITY\",        \"name\": \"Key QUALITY\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }                     ,{        \"field\": \"key.FINANCIAL\",        \"name\": \"Key FINANCIAL\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }          ]  },  \"search_fields\": {    \"name\": 1,    \"description\": 1,    \"metadataPath\": 1,    \"type\": 1,    \"subtype\": 1,    \"tenant\": 1,    \"active\": 1,    \"discoveredAt\": 1,    \"dataStore\": 1,    \"modifiedAt\": 1,    \"businessTerms\": 1,    \"keys\": 1             ,\"key.OWNER\": 1             ,\"key.QUALITY\": 1             ,\"key.FINANCIAL\": 1      }}")(res)
  }

}