package com.arjun.processer

import java.util

import com.arjun.datastructures._
import com.arjun.metadataframework.MetadataHandler
import com.arjun.utils.DataStructureIterate
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class DataFrameCaster(sourceExtractDefinition: SourceExtractDefinition
											, sourceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]
											, metadataHandler: MetadataHandler
											, targetTableDefinition: TargetTableDefinition
											, targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition]
										 ) {

	val logger = LoggerFactory.getLogger(classOf[DataFrameColumnMapper])
	var dsIterate: DataStructureIterate = _

	def cast(inputDataFrame: DataFrame): (DataFrame) = {

		logger.info("Inside cast")

		try{
			val castingSequence = generateTableCastingSelectExpr(targetTableColumnDefinition)
			val castedDf = performCast(inputDataFrame, castingSequence)
			return (castedDf)

		}catch{
			case exception: Exception => logger.info(exception.printStackTrace().toString) ; return null
		}


	}

	def performCast(inputDataFrame: DataFrame, columnCasting: Seq[Column]): DataFrame = {

		logger.info("Inside performCast")
		logger.info(s"The argument values are ${columnCasting}")
		val df = inputDataFrame.select(columnCasting: _*)

		logger.info("casting completed")
		df

	}

	def generateTableCastingSelectExpr(targetTableColumnDefinitions: util.ArrayList[TargetTableColumnDefinition]): Seq[Column] = {

		logger.info("Inside generateTableColumnMappingSelectExpr")
		var ColumnMapping: Seq[Column] = Seq.empty[Column]

		for (targetTableCoulmnDefinition: TargetTableColumnDefinition <- targetTableColumnDefinitions.asScala) {
			val targetColumnName = targetTableCoulmnDefinition.column_name.trim()
			val targetDataType = targetTableCoulmnDefinition.column_data_type
			ColumnMapping = ColumnMapping :+ expr(s" cast(  `${targetColumnName}` as  ${targetDataType}) as `${targetColumnName}` ")
		}
		ColumnMapping
	}


}
