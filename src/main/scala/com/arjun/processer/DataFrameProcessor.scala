package com.arjun.processer

import java.util
import java.util.ArrayList

import com.arjun.batchframework.BatchHandler
import com.arjun.datareader.{CsvFileReader, OrcFileReader, TsvFileReader}
import com.arjun.datastructures._
import com.arjun.datawriter.{DataWriter, ErrorDataWriter}
import com.arjun.metadataframework.MetadataHandler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.unix_timestamp

class DataFrameProcessor(sparkSession: SparkSession, batchHandler: BatchHandler, argumentHolder: ArgumentHolder) {

	def process(sourceExtractionDefinition: SourceExtractDefinition,
							sourceExtractColumnDefinitions: ArrayList[SourceExtractColumnDefinition]
							, sourceDefinition: SourceDefinition
							, metadataHandler: MetadataHandler
							, sparkSession: SparkSession): Unit = {

		// read from source directory
		val sourceDf = readDataFromSource(sourceExtractionDefinition, sourceExtractColumnDefinitions)

		val sourceCount = sourceDf.count()
		batchHandler.insertStatsTable(sourceExtractionDefinition, "input-record-count", sourceCount.toString)

		val df = sourceDf.dropDuplicates()
		df.cache()

		val targetTableDefinition: TargetTableDefinition = metadataHandler
			.readTargetTableDefinitionTable(sourceExtractionDefinition.extract_id)

		val targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition] = metadataHandler
			.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)

		val columnMappedDf = new DataFrameColumnMapper(sourceExtractionDefinition,
			sourceExtractColumnDefinitions
			, metadataHandler
			, targetTableDefinition
			, targetTableColumnDefinition).map(df)

		val transformedDf = new DataFrameTransformer(sourceExtractColumnDefinitions
			, metadataHandler
			, targetTableDefinition
			, targetTableColumnDefinition).transformData(columnMappedDf)

		val castedDf = new DataFrameCaster(sourceExtractionDefinition,
			sourceExtractColumnDefinitions
			, metadataHandler
			, targetTableDefinition
			, targetTableColumnDefinition).cast(transformedDf)

		// filter data based on rules . Currently not implemented .
		val targetColumnFilter = metadataHandler
			.readTargetColumnFilterTable(targetTableDefinition.table_id
				, s"${argumentHolder.region}_${targetTableDefinition.database_name}")

		var filterCondition = ""
		if ( targetColumnFilter != null ) {
			filterCondition = targetColumnFilter.filter_condition
		}

		val filteredDf = new DataFrameFilter().Filter(castedDf, filterCondition)

		val inputCount = castedDf.count()
		batchHandler.insertStatsTable(sourceExtractionDefinition, "input-distinct-record-count", inputCount.toString)

		//Column validation
		val validatedData
		= new DataFrameValidation(metadataHandler
			, targetTableDefinition
			, targetTableColumnDefinition
			, sparkSession)
			.validate(filteredDf)

		val filteredCount = filteredDf.count()
		batchHandler.insertStatsTable(sourceExtractionDefinition, "records-after-filter", filteredCount.toString)

		//Error Records
		val errorDf = filteredDf.except(validatedData)

		// add load timestamp column.
		val finalDf = validatedData.withColumn("load_ts", unix_timestamp().cast("timestamp"))

		val outputCount = finalDf.count()
		batchHandler.insertStatsTable(sourceExtractionDefinition, "output-record-count", outputCount.toString)

		//writing result
		var writeResult: Boolean
		= new DataWriter(targetTableDefinition, targetTableColumnDefinition, sourceExtractionDefinition, sourceDefinition, argumentHolder, sparkSession)
			.writeData(finalDf)

		val errorCount = errorDf.count()
		batchHandler.insertStatsTable(sourceExtractionDefinition, "error-record-count", errorCount.toString)

		//writing result
		writeResult = new ErrorDataWriter(targetTableDefinition
			, sourceExtractionDefinition
			, argumentHolder
			, sparkSession)
			.writeData(errorDf)

	}

	def readDataFromSource(sourceExtractionDefinition: SourceExtractDefinition
												 , sourceExtractColumnDefinitions: ArrayList[SourceExtractColumnDefinition]): DataFrame = {

		val df = sourceExtractionDefinition.extract_type.toLowerCase match {
			case "csv" => new CsvFileReader(sparkSession, argumentHolder).readData(sourceExtractionDefinition, sourceExtractColumnDefinitions)
			case "tsv" => new TsvFileReader(sparkSession, argumentHolder).readData(sourceExtractionDefinition, sourceExtractColumnDefinitions)
			case "orc" => new OrcFileReader(sparkSession, argumentHolder).readData(sourceExtractionDefinition, sourceExtractColumnDefinitions)
		}
		df
	}

}
