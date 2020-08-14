package com.arjun.processer

import java.util

import com.arjun.datastructures._
import com.arjun.metadataframework.MetadataHandler
import com.arjun.utils.DataStructureIterate
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class DataFrameColumnMapper(sourceExtractDefinition: SourceExtractDefinition
														, sourceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]
														, metadataHandler: MetadataHandler
														, targetTableDefinition: TargetTableDefinition
														, targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition]
													 ) {

	val logger = LoggerFactory.getLogger(classOf[DataFrameColumnMapper])
	var dsIterate: DataStructureIterate = _

	def map(inputDataFrame: DataFrame): (DataFrame) = {

		logger.info("Inside map")

		dsIterate = new DataStructureIterate()

		//To Do
		// Add a routine if , there is mapping that exists .
		//
		//

		val castingAndMappingSequence = generateTableColumnMappingSelectExpr(targetTableColumnDefinition)

		val mappedAndCastedDf = performMapAndCast(inputDataFrame, castingAndMappingSequence)

		//(mappedAndCastedDf, targetTableDefinition, targetTableColumnDefinitions)
		(mappedAndCastedDf)

	}

	def performMapAndCast(inputDataFrame: DataFrame, columnMapping: Seq[Column]): DataFrame = {

		logger.info("Inside performMapAndCast")
		logger.info(s"The argument values are ${columnMapping}")

		val df = inputDataFrame.select(columnMapping: _*)
		logger.info("mapping and casting completed")
		df

	}

	def generateTableColumnMappingSelectExpr(targetTableColumnDefinitions: util.ArrayList[TargetTableColumnDefinition]): Seq[Column] = {

		logger.info("Inside generateTableColumnMappingSelectExpr")
		var ColumnMapping: Seq[Column] = Seq.empty[Column]

		for (targetTableCoulmnDefinition: TargetTableColumnDefinition <- targetTableColumnDefinitions.asScala) {

			val sourceTargetColumnMapping: SourceTargetColumnMapping = metadataHandler
				.readSourceTargetColumnMappingTable(targetTableCoulmnDefinition.table_id ,targetTableCoulmnDefinition.table_column_id)


			var sourceColumnName = ""
			if ( sourceTargetColumnMapping == null ) {
				sourceColumnName = targetTableCoulmnDefinition.column_name.trim()
			} else {
				logger.info(s"sourceTargetColumnMapping is ${sourceTargetColumnMapping}")
				logger.info(s"iterating for column ${sourceTargetColumnMapping.extract_column_id}")
				sourceColumnName = dsIterate.getSourceColumnName(sourceExtractColumnDefinitions
					, sourceTargetColumnMapping.extract_column_id).trim()
			}

			val targetColumnName = targetTableCoulmnDefinition.column_name.trim()
			val targetDataType = targetTableCoulmnDefinition.column_data_type

			ColumnMapping = ColumnMapping :+ expr(s" `${sourceColumnName}` as `${targetColumnName}` ")

		}
		ColumnMapping
	}


}
