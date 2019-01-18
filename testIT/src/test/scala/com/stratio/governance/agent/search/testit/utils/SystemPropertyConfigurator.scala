package com.stratio.governance.agent.search.testit.utils

object SystemPropertyConfigurator {

  def get(orig: String, sp: String): String = {
    val spv: String = System.getProperty(sp)
    if (spv == null) {
      orig
    } else {
      spv
    }
  }

}
