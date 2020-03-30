package com.stratio.governance.agent.search.testit

import com.stratio.governance.agent.search.testit.utils.SystemPropertyConfigurator
import com.stratio.governance.agent.searcher.actors.dao.searcher.{Indexer, IndexerDomain, IndexerDomains}
import com.stratio.governance.agent.searcher.actors.manager.DGManager
import com.stratio.governance.agent.searcher.http.defimpl.DGHttpManager
import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import com.stratio.governance.agent.searcher.main.AppConf
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import org.junit.{FixMethodOrder, Test}
import org.junit.Assert._
import org.junit.runners.MethodSorters

@FixMethodOrder( MethodSorters.NAME_ASCENDING )
class SearchIT {

  val httpManager = new DGHttpManager(
    SystemPropertyConfigurator.get(AppConf.managerUrl,"MANAGER_MANAGER_URL"),
    SystemPropertyConfigurator.get(AppConf.indexerURL, "MANAGER_INDEXER_URL")
  )
  implicit val formats: DefaultFormats.type = DefaultFormats


  //"Unreacheable port " should " launch an exception" in {
  @Test
  def test01UnreacheablePort: Unit = {

    val httpManagerExc = new DGHttpManager("http://localhost:7070", "http://localhost:7070")
    try {
      httpManagerExc.getManagerModels()
      assert(false,"An exception must have been thrown")
    } catch {
      case e: HttpException => {
        e match {
          case HttpException(code, request, response) => {
            assertEquals("000",code)
            assert(response.contains("Connection refused"), "Error does not contain 'Connection Refused' message")
          }
        }
      }
    }
  }

//  TODO this case must be reviewed. Currently, it comes into a loop
//  "Unreacheable host " should " launch an exception" in {
//
//    val httpManagerExc: HttpManager = new DGHttpManager("http://11.11.11.11:7070", "http://11.11.11.11:7070")
//    try {
//      httpManagerExc.getManagerModels()
//      assert(false,"An exception must have been thrown")
//    } catch {
//      case e =>
//        e match {
//          case HttpException(code, request, response) => {
//            assertResult("000")(code)
//            assert(response.contains("Host Unreachabled"), "Error does not contain 'Host Unreachabled' message")
//          }
//          case _ =>
//            assert(false, "incorrect Exception: " + e.getStackTrace)
//        }
//    }
//
//  }

  //"Get initial Models " should " retrieve an empty list" in {
  @Test
  def test02InitialModel: Unit = {

    val reference = "{\"total\":0,\"domains\":[]}"
    val res = httpManager.getManagerModels()

    assertEquals(reference, res)

  }

  //"Create model " should " work without errors" in {
  @Test
  def test03CreateModel: Unit = {

    val res = httpManager.insertOrUpdateModel(DGManager.MODEL_NAME,"{\"name\":\"Governance Search\",\"model\":{\"id\":\"id\",\"language\":\"english\",\"search_fuzzy_enabled\":true,\"search_ngram_enabled\":true,\"autocomplete_phonetic_enabled\":false,\"search_phonetic_enabled\":false,\"fields\":[{\"field\":\"id\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"name\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false,\"autocompletable\":1},{\"field\":\"alias\",\"name\":\"Alias\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false,\"autocompletable\":1},{\"field\":\"description\",\"name\":\"Description\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"metadataPath\",\"name\":\"Metadata path\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"type\",\"name\":\"Source type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"subtype\",\"name\":\"Asset type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"tenant\",\"name\":\"Tenant\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"active\",\"name\":\"Active\",\"type\":\"boolean\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"discoveredAt\",\"name\":\"Access time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"modifiedAt\",\"name\":\"Access time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"dataStore\",\"name\":\"Source\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"businessTerms\",\"name\":\"Business terms\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"keys\",\"name\":\"Keys\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.OWNER\",\"name\":\"Key OWNER\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.QUALITY\",\"name\":\"Key QUALITY\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.FINANCIAL\",\"name\":\"Key FINANCIAL\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true}]},\"search_fields\":{\"name\":10,\"alias\":1000,\"description\":70,\"type\":1,\"subtype\":1,\"tenant\":1,\"discoveredAt\":1,\"modifiedAt\":1,\"businessTerms\":50,\"keys\":50,\"key.OWNER\":50,\"key.QUALITY\":50,\"key.FINANCIAL\":50}}")

    // Getting here is to work well
    assert(true)

  }

  //"Partial indexation " should " index a document without errors" in {
  @Test
  def test04PartialIndexationWithErrors: Unit = {

    val data = "[{\"id\":\"1\",\"name\":\"HdfsStore\",\"description\":\"Empty DataStore\",\"metadataPath\":\"emptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"Table\",\"tenant\":\"stratio\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\"}]"

    val res = httpManager.partialPostRequest(DGManager.MODEL_NAME, data)
    val json = parse(res)
    val stats = json.extract[Indexer]

    assertEquals(0, stats.documents_stats.error)
    assertEquals(1, stats.documents_stats.created)

  }

  //"Get Indexer Domains" should " Retrieve a list of domains with status ENDED and no token" in {
  @Test
  def test05IndexerDomains: Unit = {

    val res = httpManager.getIndexerdomains()
    val json = parse(res)
    val domains = json.extract[IndexerDomains]

    assertEquals(1, domains.domains.size)
    assertEquals(Some("ENDED"), domains.domains.head.status)
    assertEquals(None, domains.domains.head.token)

  }

  //"Total Indexation Error case " should " abort total indexation process" in {
  @Test
  def test06TotalIndexationErrorCase: Unit = {

    val init = httpManager.initTotalIndexationProcess(DGManager.MODEL_NAME)
    val jsonInit = parse(init)
    val domain = jsonInit.extract[IndexerDomain]

    assertEquals(Some("INDEXING"), domain.status)

    val token = domain.token
    assert(token.isDefined)

    httpManager.cancelTotalIndexationProcess(DGManager.MODEL_NAME, token.get)

    // Getting here is to work well
    assert(true)

  }

  //"Total Indexation Succeed case " should " persist three registers" in {
  @Test
  def test07TotalIndexationSucceedCase: Unit = {

    // Init process
    val init = httpManager.initTotalIndexationProcess(DGManager.MODEL_NAME)
    val jsonInit = parse(init)
    val domain = jsonInit.extract[IndexerDomain]

    assertEquals(Some("INDEXING"), domain.status)

    val token = domain.token
    assert(token.isDefined)

    httpManager.insertOrUpdateModel(DGManager.MODEL_NAME,"{\"name\":\"Governance Search\",\"model\":{\"id\":\"id\",\"language\":\"english\",\"search_fuzzy_enabled\":true,\"search_ngram_enabled\":true,\"autocomplete_phonetic_enabled\":false,\"search_phonetic_enabled\":false,\"fields\":[{\"field\":\"id\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"name\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false,\"autocompletable\":1},{\"field\":\"alias\",\"name\":\"Alias\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false,\"autocompletable\":1},{\"field\":\"description\",\"name\":\"Description\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"metadataPath\",\"name\":\"Metadata path\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"type\",\"name\":\"Source type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"subtype\",\"name\":\"Asset type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"tenant\",\"name\":\"Tenant\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"active\",\"name\":\"Active\",\"type\":\"boolean\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"discoveredAt\",\"name\":\"Access time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"modifiedAt\",\"name\":\"Access time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"dataStore\",\"name\":\"Source\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"businessTerms\",\"name\":\"Business terms\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"keys\",\"name\":\"Keys\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.OWNER\",\"name\":\"Key OWNER\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.QUALITY\",\"name\":\"Key QUALITY\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.FINANCIAL\",\"name\":\"Key FINANCIAL\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true}]},\"search_fields\":{\"name\":10,\"alias\":1000,\"description\":70,\"type\":1,\"subtype\":1,\"tenant\":1,\"discoveredAt\":1,\"modifiedAt\":1,\"businessTerms\":50,\"keys\":50,\"key.OWNER\":50,\"key.QUALITY\":50,\"key.FINANCIAL\":500}}")

    // Inject data
    val data = "[{\"id\":\"192\",\"name\":\"R_REGIONKEY\",\"description\":\"Hdfs parquet column\",\"metadataPath\":\"hdfsFinance:///department/finance/2018>/:region.parquet:R_REGIONKEY:\",\"type\":\"HDFS\",\"subtype\":\"Column\",\"tenant\":\"NONE\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\"},{\"id\":\"194\",\"name\":\"finance\",\"description\":\"finance Hdfs directory\",\"metadataPath\":\"hdfsFinance://department>/finance:\",\"type\":\"HDFS\",\"subtype\":\"Path\",\"tenant\":\"NONE\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\",\"keys\":[\"OWNER\"],\"key.OWNER\":\"audit\"},{\"id\":\"195\",\"name\":\"R_REGIONKEY\",\"description\":\"Hdfs parquet column\",\"metadataPath\":\"hdfsFinance:///department/finance/2017>/:region.parquet:R_REGIONKEY\",\"type\":\"HDFS\",\"subtype\":\"Column\",\"tenant\":\"NONE\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\",\"businessTerms\":[\"Financial\"],\"keys\":[\"OWNER\"],\"key.OWNER\":\"audit\"}]"
    val res = httpManager.totalPostRequest(DGManager.MODEL_NAME, token.get, data)
    val json = parse(res)
    val stats = json.extract[Indexer]

    assertEquals(0, stats.documents_stats.error)
    assertEquals(3, stats.documents_stats.created)

    // Finish process
    httpManager.finishTotalIndexationProcess(DGManager.MODEL_NAME, token.get)

    // Getting here is to work well
    assert(true)

  }

}