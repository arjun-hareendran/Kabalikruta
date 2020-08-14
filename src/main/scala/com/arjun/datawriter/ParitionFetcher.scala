package com.arjun.datawriter

import java.util.Date

import com.arjun.datastructures.TargetTableDefinition
import java.text.SimpleDateFormat


class ParitionFetcher(targetTableDefinition: TargetTableDefinition) {


  def getParition(): String = {

    val date = new Date();
    val paritionValue = targetTableDefinition.partition match {
      case "yyyy-MM-dd" => s"${targetTableDefinition.table_name}/" +
        s"${new SimpleDateFormat("yyyy-MM-dd").format(date).toString}"

      case _ => s"/${targetTableDefinition.table_name}/"
    }

    paritionValue
  }

}
