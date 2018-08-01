package com.stratio.governance.agent.searcher.model.utils

import java.text.SimpleDateFormat
import java.util.Date

object Utils {

  val DATE_FORMAT   = "yyyy-MM-dd HH:mm:ss.SSS"
  val DATE_FORMAT_T = "yyyy-MM-dd'T'HH:mm:ss.SSS"

  def getDateAsString(d: Date, format: String): String = {
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.format(d)
  }

  def convertStringToDate(s: String, format: String): Date = {
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.parse(s)
  }

}
