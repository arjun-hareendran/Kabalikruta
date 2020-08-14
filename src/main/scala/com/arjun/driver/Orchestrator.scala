package com.arjun.driver

import java.sql.{Connection, ResultSet, Statement}
import java.util

import com.arjun.batchframework.BatchHandler
import com.arjun.datastructures.{ArgumentHolder, SourceExtractDefinition}
import com.arjun.functions.UserDefinedFunctions
import com.arjun.metadataframework.MetadataHandler
import com.arjun.processer.{ArgumentMapper, DataFrameProcessor}
import com.arjun.sqlconnection.SqlConnection
import com.arjun.validation.InputArgumentChecker
import org.apache.spark.sql.{Column, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class Orchestrator {

	val logger = LoggerFactory.getLogger(classOf[Orchestrator])

	//class variable definitions
	var argumentMapper: ArgumentMapper = _
	var sqlConnection: Connection = _
	var connectionObject: SqlConnection = _
	var batchHandler: BatchHandler = _
	var argumentHolder: ArgumentHolder = _
	var sparkSession: SparkSession = _
	var sqlStatement: Statement = _


	def setUp(inputArguments: Array[String]): Option[Exception] = {

		logger.info("Inside setUp")

		//set up sql Connection
		connectionObject = new SqlConnection()
		sqlConnection = connectionObject.connectionRouter(argumentHolder.region)
		sqlStatement = sqlConnection.createStatement()

		batchHandler = new BatchHandler(sqlStatement,argumentHolder.region)

		//set up sql Connection
		sparkSession = SparkSession
			.builder
			.master("yarn")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		val udfs = new UserDefinedFunctions()
		sparkSession = registerUdfs(udfs, sparkSession)


		None
	}

	def registerUdfs(udfs: UserDefinedFunctions, sparkSession: SparkSession): SparkSession = {

		sparkSession.udf.register("formattime", udfs.formattime(_: String))

		sparkSession
	}

	def process(inputArguments: Array[String]): Option[Exception] = {

		logger.info("Inside process")


		if ( !processArguments(inputArguments) ) {
			throw new Exception("Invalid Input Arguments")
		}

		batchHandler.insertBatchTable(argumentHolder.sourceId)

		try {
			processFiles()
			batchHandler.updateBatchTable(argumentHolder.sourceId, "completed")

		} catch {
			case exception: Exception => {
				logger.info(exception.printStackTrace().toString)
				batchHandler.updateBatchTable(argumentHolder.sourceId, "failed")

			}
		}

		None
	}

	def processArguments(inputArguments: Array[String]): Boolean = {

		logger.info("Inside processArguments")


		if ( !checkArguments(inputArguments) ) {
			return false
		}

		argumentMapper = new ArgumentMapper()
		argumentHolder = argumentMapper.mapCommandLineArguments(inputArguments = inputArguments)

		return true
	}

	def checkArguments(arguments: Array[String]): Boolean = {

		logger.info("Inside checkArguments")
		val inputArgumentChecker = new InputArgumentChecker()
		inputArgumentChecker.checkArguments(arguments)

	}


	def processFiles() = {
		logger.info("Inside processFiles")

		//understand which source we want to process
		val metadataHandler = new MetadataHandler(sqlStatement, argumentHolder)

		//get source related information
		val sourceDefinition = metadataHandler.readSourceDefintionTable(argumentHolder.sourceId)

		//get the extract in the source
		val souceExrtractDefinitions: util.ArrayList[SourceExtractDefinition] = metadataHandler.readSourceExtractDefinitionTable(sourceDefinition.data_source_id)

		//process one extract one by one
		for (sourceExtractionDefinition: SourceExtractDefinition <- souceExrtractDefinitions.asScala) {

			// entry for each extract
			batchHandler.insertJobTable(sourceExtractionDefinition)

			//read extract column definition
			val soruceExtractColumnDefinitions = metadataHandler.readSoruceExtractColumnDefinition(sourceExtractionDefinition.extract_id)

			val dataframeProcessor = new DataFrameProcessor(sparkSession, batchHandler, argumentHolder)

			try {
				dataframeProcessor.process(sourceExtractionDefinition, soruceExtractColumnDefinitions, sourceDefinition, metadataHandler, sparkSession)
			} catch {
				case exception: Exception => {
					batchHandler.updateJobTable(sourceExtractionDefinition, "failed")
					throw exception
				}
			}

			batchHandler.updateJobTable(sourceExtractionDefinition, "completed")


		}
	}
}
