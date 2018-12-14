package com.stratio.governance.agent.search.testit


import com.stratio.governance.agent.search.testit.utils.Configuration
import com.stratio.governance.agent.searcher.actors.dao.searcher.{IndexerDomain, IndexerDomains, Indexer}
import com.stratio.governance.agent.searcher.http.defimpl.DGHttpManager
import com.stratio.governance.agent.searcher.http.{HttpException, HttpManager}
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import org.scalatest.FlatSpec

class SearchITTest extends FlatSpec {

  val httpManager: HttpManager = new DGHttpManager(Configuration.MANAGER_URL, Configuration.INDEXER_URL)
  implicit val formats: DefaultFormats.type = DefaultFormats

  "Unreacheable port " should " launch an exception" in {

    val httpManagerExc: HttpManager = new DGHttpManager("http://localhost:7070", "http://localhost:7070")
    try {
      httpManagerExc.getManagerModels()
      assert(false,"An exception must have been thrown")
    } catch {
      case e: HttpException => {
        e match {
          case HttpException(code, request, response) => {
            assertResult("000")(code)
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

  "Get initial Models " should " retrieve an empty list" in {

    val reference = "{\"total\":0,\"domains\":[]}"
    val res: String = httpManager.getManagerModels()

    assertResult(reference)(res)

  }

  "Create model " should " work without errors" in {

    val res = httpManager.insertOrUpdateModel(Configuration.MODEL,"{\"name\":\"Governance Search Test\",\"model\":{\"id\":\"id\",\"language\":\"spanish\",\"fields\":[{\"field\":\"id\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"name\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"description\",\"name\":\"Description\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"metadataPath\",\"name\":\"Metadata Path\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"type\",\"name\":\"Data Stores\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"subtype\",\"name\":\"Type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"tenant\",\"name\":\"Tenant\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"active\",\"name\":\"Active\",\"type\":\"boolean\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"discoveredAt\",\"name\":\"Access Time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"modifiedAt\",\"name\":\"Access Time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"businessTerms\",\"name\":\"Business Terms\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"qualityRules\",\"name\":\"Quality Rules\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"keys\",\"name\":\"Keys\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.OWNER\",\"name\":\"key.OWNER\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.QUALITY\",\"name\":\"key.QUALITY\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false}]},\"search_fields\":{\"name\":1,\"description\":1,\"metadataPath\":1,\"type\":1,\"subtype\":1,\"tenant\":1,\"active\":1,\"discoveredAt\":1,\"modifiedAt\":1,\"businessTerms\":1,\"qualityRules\":1,\"keys\":1,\"key.OWNER\":1,\"key.QUALITY\":1}}")

    // Getting here is to work well
    assert(true)

  }

  "Partial indexation " should " index a document without errors" in {

    val data = "[{\"id\":1,\"name\":\"HdfsStore\",\"description\":\"Empty DataStore\",\"metadataPath\":\"emptyDatastore:\",\"type\":\"HDFS\",\"subtype\":\"Table\",\"tenant\":\"stratio\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\"}]";

    val res = httpManager.partialPostRequest(Configuration.MODEL, data)
    val json = parse(res)
    val stats: Indexer  = json.extract[Indexer]

    assertResult(0)(stats.documents_stats.error)
    assertResult(1)(stats.documents_stats.created)

  }

  "Get Indexer Domains" should " Retrieve a list of domains with status ENDED and no token" in {

    val res = httpManager.getIndexerdomains()
    val json = parse(res)
    val domains: IndexerDomains  = json.extract[IndexerDomains]

    assertResult(1)(domains.domains.size)
    assertResult(Some("ENDED"))(domains.domains.head.status)
    assertResult(None)(domains.domains.head.token)

  }

  "Total Indexation Error case " should " abort total indexation process" in {

    val init = httpManager.initTotalIndexationProcess(Configuration.MODEL)
    val jsonInit = parse(init)
    val domain: IndexerDomain  = jsonInit.extract[IndexerDomain]

    assertResult(Some("INDEXING"))(domain.status)

    val token = domain.token
    assert(token.isDefined)

    httpManager.cancelTotalIndexationProcess(Configuration.MODEL, token.get)

    // Getting here is to work well
    assert(true)

  }

  "Total Indexation Succeed case " should " persist three registers" in {

    // Init process
    val init = httpManager.initTotalIndexationProcess(Configuration.MODEL)
    val jsonInit = parse(init)
    val domain: IndexerDomain  = jsonInit.extract[IndexerDomain]

    assertResult(Some("INDEXING"))(domain.status)

    val token = domain.token
    assert(token.isDefined)

    httpManager.insertOrUpdateModel(Configuration.MODEL,"{\"name\":\"Governance Search Test\",\"model\":{\"id\":\"id\",\"language\":\"spanish\",\"fields\":[{\"field\":\"id\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"name\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"description\",\"name\":\"Description\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"metadataPath\",\"name\":\"Metadata Path\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"type\",\"name\":\"Data Stores\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"subtype\",\"name\":\"Type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"tenant\",\"name\":\"Tenant\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"active\",\"name\":\"Active\",\"type\":\"boolean\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"discoveredAt\",\"name\":\"Access Time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"modifiedAt\",\"name\":\"Access Time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"businessTerms\",\"name\":\"Business Terms\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"qualityRules\",\"name\":\"Quality Rules\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"keys\",\"name\":\"Keys\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.OWNER\",\"name\":\"key.OWNER\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.QUALITY\",\"name\":\"key.QUALITY\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true}]},\"search_fields\":{\"name\":1,\"description\":1,\"metadataPath\":1,\"type\":1,\"subtype\":1,\"tenant\":1,\"active\":1,\"discoveredAt\":1,\"modifiedAt\":1,\"businessTerms\":1,\"qualityRules\":1,\"keys\":1,\"key.OWNER\":3,\"key.QUALITY\":3}}")

    // Inject data
    val data: String = "[{\"id\":192,\"name\":\"R_REGIONKEY\",\"description\":\"Hdfs parquet column\",\"metadataPath\":\"hdfsFinance:///department/finance/2018>/:region.parquet:R_REGIONKEY:\",\"type\":\"HDFS\",\"subtype\":\"Column\",\"tenant\":\"NONE\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\"},{\"id\":194,\"name\":\"finance\",\"description\":\"finance Hdfs directory\",\"metadataPath\":\"hdfsFinance://department>/finance:\",\"type\":\"HDFS\",\"subtype\":\"Path\",\"tenant\":\"NONE\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\",\"keys\":[\"OWNER\"],\"key.OWNER\":\"audit\"},{\"id\":195,\"name\":\"R_REGIONKEY\",\"description\":\"Hdfs parquet column\",\"metadataPath\":\"hdfsFinance:///department/finance/2017>/:region.parquet:R_REGIONKEY\",\"type\":\"HDFS\",\"subtype\":\"Column\",\"tenant\":\"NONE\",\"dicoveredAt\":\"2018-09-28T20:45:00.000\",\"modifiedAt\":\"2018-09-28T20:45:00.000\",\"businessTerms\":[\"Financial\"],\"keys\":[\"OWNER\"],\"key.OWNER\":\"audit\"}]"
    val res = httpManager.totalPostRequest(Configuration.MODEL, token.get, data)
    val json = parse(res)
    val stats: Indexer  = json.extract[Indexer]

    assertResult(0)(stats.documents_stats.error)
    assertResult(3)(stats.documents_stats.created)

    // Finish process
    httpManager.finishTotalIndexationProcess(Configuration.MODEL, token.get)

    // Getting here is to work well
    assert(true)

  }
}