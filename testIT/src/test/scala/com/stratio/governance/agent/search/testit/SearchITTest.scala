import com.stratio.governance.agent.search.testit.Configuration
import com.stratio.governance.agent.searcher.http.HttpManager
import com.stratio.governance.agent.searcher.http.defimpl.DGHttpManager
import org.scalatest.FlatSpec

class SearchITTest extends FlatSpec {

  val httpManager: HttpManager = new DGHttpManager(Configuration.MANAGER_URL, Configuration.INDEXER_URL, Configuration.SSL)

  "Get initial Models " should " retrieve an empty list" in {

    val reference = "{\"total\":0,\"domains\":[]}"
    val res: String = httpManager.getManagerModels()

    assertResult(reference)(res)

  }

  "Create model " should " work without errors" in {

    val res = httpManager.insertOrUpdateModel("governance_search_test","{\"name\":\"Governance Search Test\",\"model\":{\"id\":\"id\",\"language\":\"spanish\",\"fields\":[{\"field\":\"id\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":false,\"sortable\":false,\"aggregable\":false},{\"field\":\"name\",\"name\":\"Name\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"description\",\"name\":\"Description\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"metadataPath\",\"name\":\"Metadata Path\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"type\",\"name\":\"Data Stores\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"subtype\",\"name\":\"Type\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"tenant\",\"name\":\"Tenant\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"active\",\"name\":\"Active\",\"type\":\"boolean\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"discoveredAt\",\"name\":\"Access Time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"modifiedAt\",\"name\":\"Access Time\",\"type\":\"date\",\"format\":\"yyyy-MM-dd'T'HH:mm:ss.SSS\",\"searchable\":true,\"sortable\":true,\"aggregable\":false},{\"field\":\"businessTerms\",\"name\":\"Business Terms\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"qualityRules\",\"name\":\"Quality Rules\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"keys\",\"name\":\"Keys\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.OWNER\",\"name\":\"key.OWNER\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":true},{\"field\":\"key.QUALITY\",\"name\":\"key.QUALITY\",\"type\":\"text\",\"searchable\":true,\"sortable\":true,\"aggregable\":false}]},\"search_fields\":{\"name\":1,\"description\":1,\"metadataPath\":1,\"type\":1,\"subtype\":1,\"tenant\":1,\"active\":1,\"discoveredAt\":1,\"modifiedAt\":1,\"businessTerms\":1,\"qualityRules\":1,\"keys\":1,\"key.OWNER\":1,\"key.QUALITY\":1}}")

    // Getting here is to work well
    assert(true)

  }

}