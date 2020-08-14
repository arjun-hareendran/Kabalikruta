package com.arjun.datawriter

import com.arjun.datastructures._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class ErrorDataWriter(targetTableDefinition: TargetTableDefinition
											, sourceExtractionDefinition: SourceExtractDefinition
											, argumentHolder: ArgumentHolder
											, sparkSession: SparkSession
										 ) {
	val logger = LoggerFactory.getLogger(classOf[ErrorDataWriter])

	def writeData(errroDataFrame: DataFrame) = {

		logger.info("Inside writeData")

		// To do .replace with a better logic
		val location = targetTableDefinition.table_location.replace("currated", "error") + s"/${argumentHolder.partitionDate}"

		logger.info(s"Error Location is ${location}")

		errroDataFrame.write
			.format("com.databricks.spark.csv")
			.mode("overwrite")
			.save(location)

		true
	}

}
