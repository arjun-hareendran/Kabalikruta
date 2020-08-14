package com.arjun.utils

import java.util

import com.arjun.datastructures.SourceExtractColumnDefinition
import com.arjun.processer.DataFrameColumnMapper
import org.slf4j.LoggerFactory
import collection.JavaConverters._


class DataStructureIterate {


  val logger = LoggerFactory.getLogger(classOf[DataStructureIterate])


  def getSourceColumnName(sourceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]
                          , source_column_definition_id: String): String = {

    logger.info("Inside getSourceColumnName")
    logger.info(s"The argument values are ${sourceExtractColumnDefinitions} , ${source_column_definition_id}")

    var columnName = ""
    for (sourceExtractColumnDefinition: SourceExtractColumnDefinition <- sourceExtractColumnDefinitions.asScala) {

      if ( sourceExtractColumnDefinition.extract_column_id == source_column_definition_id ) {
        columnName = sourceExtractColumnDefinition.column_name
      }
    }

    if ( columnName.equalsIgnoreCase("") ) {
      throw new Exception("Column name not found")
    } else {
      logger.debug(s"The value returned is ${columnName}")
      return columnName
    }
  }
}
