package com.arjun.datareader

import java.util

import com.arjun.batchframework._
import com.arjun.datastructures.{ArgumentHolder, SourceExtractColumnDefinition, SourceExtractDefinition}
import com.arjun.driver.Orchestrator
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class CsvFileReader(sparkSession: SparkSession ,argumentHolder:ArgumentHolder) {

  val logger = LoggerFactory.getLogger(classOf[CsvFileReader])

  def readData(sourceExtractionDefinition: SourceExtractDefinition, soruceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]): DataFrame = {

    logger.info("Inside readData")

    val schema = generateSchema(soruceExtractColumnDefinitions)


    val partitionDate = argumentHolder.partitionDate
    val dataLocation = s"${sourceExtractionDefinition.extract_location}/${partitionDate}/${sourceExtractionDefinition.extract_name}"

    val df =
      sparkSession.read.format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("escape", "\"")
        .option("multiLine", true)
        .option("ignoreTrailingWhiteSpace", true)
        .load(dataLocation)

    df
  }


  def generateSchema(soruceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]): Unit = {

    //To Do
    //Generate teh schema on the fly

  }
}
