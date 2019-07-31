package com.stratio.governance.agent.searcher.actors.manager.utils.defimpl

import com.stratio.governance.agent.searcher.actors.manager.dao.SourceDao
import com.stratio.governance.agent.searcher.actors.manager.scheduler.Scheduler
import com.stratio.governance.agent.searcher.actors.manager.utils.{ManagerUtils, ManagerUtilsException}
import org.slf4j.{Logger, LoggerFactory}

class DGManagerUtils(scheduler: Scheduler, sourceDao: SourceDao, relevance: List[String]) extends ManagerUtils {

  private lazy val LOG: Logger = LoggerFactory.getLogger(getClass.getName)

  val DOMAIN_TEMPLATE: String = "se-manager/governance_domain_template.json"

  @throws(classOf[ManagerUtilsException])
  override def getGeneratedModel(): String = {
    val domain_template: String = loadResource(DOMAIN_TEMPLATE)
      .replace(RelevanceRef.RELEVANCE_ALIAS_REF._2, relevance(RelevanceRef.RELEVANCE_ALIAS_REF._3))
      .replace(RelevanceRef.RELEVANCE_NAME_REF._2, relevance(RelevanceRef.RELEVANCE_NAME_REF._3))
      .replace(RelevanceRef.RELEVANCE_DESCRIPTION_REF._2, relevance(RelevanceRef.RELEVANCE_DESCRIPTION_REF._3))
      .replace(RelevanceRef.RELEVANCE_BUSINESSTERM_REF._2, relevance(RelevanceRef.RELEVANCE_BUSINESSTERM_REF._3))
      .replace(RelevanceRef.RELEVANCE_QUALITYRULES_REF._2, relevance(RelevanceRef.RELEVANCE_QUALITYRULES_REF._3))
      .replace(RelevanceRef.RELEVANCE_KEY_REF._2, relevance(RelevanceRef.RELEVANCE_KEY_REF._3))
      .replace(RelevanceRef.RELEVANCE_VALUE_REF._2, relevance(RelevanceRef.RELEVANCE_VALUE_REF._3))

    val res: String = domain_template.replace("\n","")
    res
  }

  @throws(classOf[ManagerUtilsException])
  override def getScheduler(): Scheduler = {
    scheduler
  }

  private def loadResource(filename: String): String = {
    val source = scala.io.Source.fromInputStream(getClass().getClassLoader().getResourceAsStream(filename))
    try source.mkString finally source.close()
  }

}

object RelevanceRef {

  val RELEVANCE_ALIAS_REF: (String, String, Int)= ("manager.relevance.alias","RELEVANCE_ALIAS",0)
  val RELEVANCE_NAME_REF: (String, String, Int)= ("manager.relevance.name","RELEVANCE_NAME",1)
  val RELEVANCE_DESCRIPTION_REF: (String, String, Int)= ("manager.relevance.description","RELEVANCE_DESCRIPTION",2)
  val RELEVANCE_BUSINESSTERM_REF: (String, String, Int)= ("manager.relevance.businessterm","RELEVANCE_BUSINESSTERM",3)
  val RELEVANCE_QUALITYRULES_REF: (String, String, Int)= ("manager.relevance.qualityRules","RELEVANCE_QUALITYRULES",4)
  val RELEVANCE_KEY_REF: (String, String, Int)= ("manager.relevance.key","RELEVANCE_KEY",5)
  val RELEVANCE_VALUE_REF: (String, String, Int)= ("manager.relevance.value","RELEVANCE_VALUE",6)

}