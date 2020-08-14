package com.arjun.datawriter

import java.util

import com.arjun.datastructures._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.util.SizeEstimator
import org.slf4j.LoggerFactory

class DataWriter(targetTableDefinition: TargetTableDefinition
								 , targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition]
								 , sourceExtractionDefinition: SourceExtractDefinition
								 , sourceDefinition: SourceDefinition, argumentHolder: ArgumentHolder,sparkSession:SparkSession) {

	val logger = LoggerFactory.getLogger(classOf[DataWriter])

	def writeData(inputDataFrame: DataFrame): Boolean = {

		logger.info("Inside writeData")
		val noOfPartitions = estimatePartitionSize(inputDataFrame)
		val coalescedDataFrame = inputDataFrame.coalesce(2)
		writeDataCurated(coalescedDataFrame)

		true
	}

	def estimatePartitionSize(inputDataFrame: DataFrame): Unit = {

		logger.info("Inside estimatePartitionSize")

		//writeDataStaging(inputDataFrame)
		val MEGABYTE: Long = 1024L * 1024L
		val totalSizeInBytes = SizeEstimator.estimate(inputDataFrame)
		val totalSizeInMB = totalSizeInBytes / MEGABYTE

		var noOfPartitions = 0L
		if ( totalSizeInMB % 200 == 0 ) {
			noOfPartitions = (totalSizeInMB / 200)
		} else {
			noOfPartitions = (totalSizeInMB / 200) + 1
		}
		noOfPartitions
	}


	def writeDataStaging(inputDataFrame: DataFrame): Boolean = {

		logger.info("Inside writeDataStaging")

		//val deltaTableWriter = new DeltaFileWriter()
		// Check if table exits of not and write routine to generate the tables.
		//new DeltaFileWriter().write(targetTableDefinition,inputDataFrame)

		true

	}

	def writeDataCurated(inputDataFrame: DataFrame): Unit = {

		logger.info("Inside writeDataCurated")

		targetTableDefinition.table_format.toLowerCase match {
			case "parquet" => new ParquetFileWriter(sparkSession).write(sourceExtractionDefinition, targetTableDefinition, inputDataFrame, argumentHolder.region)
			case _ => new ParquetFileWriter(sparkSession).write(sourceExtractionDefinition, targetTableDefinition, inputDataFrame, argumentHolder.region)
		}
	}

}
