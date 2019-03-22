package com.stratio.governance.agent.searcher.actors.manager

import com.stratio.governance.agent.searcher.actors.dao.searcher.{DGSearcherDao, DGSearcherDaoException}
import com.stratio.governance.agent.searcher.actors.manager.dao.{Available, Busy, SourceDao}
import com.stratio.governance.agent.searcher.actors.manager.utils.defimpl.DGManagerUtils
import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import com.stratio.governance.agent.searcher.main.AppConf
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

class DGManagerJsonUnitTest extends FlatSpec {

  val httpManager: HttpManager = new HttpManagerMock()
  val sourceDao: SourceDao = new SourceDaoMock()
  val managerUtils = new DGManagerUtils(null, sourceDao, List[Int](AppConf.managerRelevanceAlias, AppConf.managerRelevanceName, AppConf.managerRelevanceDescription, AppConf.managerRelevanceBusinessterm, AppConf.managerRelevanceQualityRules, AppConf.managerRelevanceKey, AppConf.managerRelevanceValue).map(i => i.toString))
  val searchDao = new DGSearcherDao(httpManager)

  "First checkTotalIndexation " should " parse true case" in {
    val res = searchDao.checkTotalIndexation("movies")
    assertResult(Busy)(res._1)
    assertResult(Some("3e33463a-2ed9-4c15-87c0-1a520e3c1cb2"))(res._2)
  }

  "Second checkTotalIndexation " should " parse false case" in {
    val res = searchDao.checkTotalIndexation("governance_search")
    assertResult(Available)(res._1)
    assertResult(None)(res._2)
  }

  "Third checkTotalIndexation " should " parse null case" in {
    val res = searchDao.checkTotalIndexation("otherthing")
    assertResult(Available)(res._1)
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
    assertResult("{  \"name\": \"Governance Search\",  \"model\": {    \"id\": \"id\",    \"language\": \"english\",    \"search_fuzzy_enabled\": true,    \"search_ngram_enabled\": true,    \"autocomplete_phonetic_enabled\": true,    \"search_phonetic_enabled\": true,    \"fields\": [      {        \"field\": \"id\",        \"name\": \"Name\",        \"type\": \"text\",        \"searchable\": false,        \"sortable\": false,        \"aggregable\": false      },      {        \"field\": \"name\",        \"name\": \"Name\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false,        \"autocompletable\": 1.0      },      {        \"field\": \"alias\",        \"name\": \"Alias\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false,        \"autocompletable\": 1.0      },      {        \"field\": \"description\",        \"name\": \"Description\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"metadataPath\",        \"name\": \"Metadata path\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"type\",        \"name\": \"Source type\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"subtype\",        \"name\": \"Asset type\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"tenant\",        \"name\": \"Tenant\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"active\",        \"name\": \"Active\",        \"type\": \"boolean\",        \"searchable\": false,        \"sortable\": false,        \"aggregable\": false      },      {        \"field\": \"discoveredAt\",        \"name\": \"Access time\",        \"type\": \"date\",        \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"modifiedAt\",        \"name\": \"Access time\",        \"type\": \"date\",        \"format\": \"yyyy-MM-dd'T'HH:mm:ss.SSS\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": false      },      {        \"field\": \"dataStore\",        \"name\": \"Source\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"businessTerms\",        \"name\": \"Business terms\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"qualityRules\",        \"name\": \"Quality rules\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      },      {        \"field\": \"keys\",        \"name\": \"Keys\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }                     ,{        \"field\": \"key.OWNER\",        \"name\": \"Key OWNER\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }                     ,{        \"field\": \"key.QUALITY\",        \"name\": \"Key QUALITY\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }                     ,{        \"field\": \"key.FINANCIAL\",        \"name\": \"Key FINANCIAL\",        \"type\": \"text\",        \"searchable\": true,        \"sortable\": true,        \"aggregable\": true      }          ]  },  \"search_fields\": {    \"name\": 10,    \"alias\": 1000,    \"description\": 70,    \"type\": 1,    \"subtype\": 1,    \"tenant\": 1,    \"discoveredAt\": 1,    \"modifiedAt\": 1,    \"businessTerms\": 50,    \"qualityRules\": 50,    \"keys\": 50             ,\"key.OWNER\": 50             ,\"key.QUALITY\": 50             ,\"key.FINANCIAL\": 50      }}")(res)
  }

}