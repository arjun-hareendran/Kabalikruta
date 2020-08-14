package com.arjun.datareader

import java.util

import com.arjun.datastructures.{ArgumentHolder, SourceExtractColumnDefinition, SourceExtractDefinition}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class OrcFileReader(sparkSession: SparkSession, argumentHolder: ArgumentHolder) {

	val logger = LoggerFactory.getLogger(classOf[OrcFileReader])

	def readData(sourceExtractionDefinition: SourceExtractDefinition, soruceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]): DataFrame = {

		logger.info("Inside readData")

		val partitionDate = argumentHolder.partitionDate
		val dataLocation = s"${sourceExtractionDefinition.extract_location}/${partitionDate}/${sourceExtractionDefinition.extract_name}"

		val df =
			sparkSession.read.format("orc")
				.load(dataLocation)

		df
	}


}
