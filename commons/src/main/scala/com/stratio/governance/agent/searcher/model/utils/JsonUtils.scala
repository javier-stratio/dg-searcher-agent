package com.stratio.governance.agent.searcher.model.utils

import org.json4s._
import org.json4s.jackson.JsonMethods._

object JsonUtils {

  def jsonStrToMap(jsonStr: String): Map[String, Any] = {
    implicit val formats: DefaultFormats = org.json4s.DefaultFormats

    parse(jsonStr).extract[Map[String, Any]]
  }
}
