package com.arjun.processer

import java.util

import com.arjun.datastructures.{FunctionsMapping, SourceExtractColumnDefinition, TargetTableColumnDefinition, TargetTableDefinition}
import com.arjun.metadataframework.MetadataHandler
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.expr
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


class DataFrameTransformer(soruceExtractColumnDefinitions: util.ArrayList[SourceExtractColumnDefinition]
													 , metadataHandler: MetadataHandler
													 , targetTableDefinition: TargetTableDefinition
													 , targetTableColumnDefinitions: util.ArrayList[TargetTableColumnDefinition]) {


	val logger = LoggerFactory.getLogger(classOf[DataFrameTransformer])

	def transformData(inputDataframe: DataFrame): DataFrame = {

		logger.info("Inside transformData")

		var df = inputDataframe
		var functionSequence = Seq.empty[Column]

		for (targetTableColumnDefinition: TargetTableColumnDefinition <- targetTableColumnDefinitions.asScala) {

			logger.debug(s"processing column ${targetTableColumnDefinition}")

			val functionsMapping: util.ArrayList[FunctionsMapping] = metadataHandler.readFunctionMappingTable(targetTableColumnDefinition.table_column_id, targetTableColumnDefinition.table_id)

			if ( functionsMapping != null ) {
				functionSequence = functionSequence :+ expr(geneateFunctionExpression(functionsMapping, targetTableColumnDefinition.column_name))
			} else {
				functionSequence = functionSequence :+ expr(s"`${targetTableColumnDefinition.column_name}` as `${targetTableColumnDefinition.column_name}`")
			}
		}

		logger.info(functionSequence.toList.toString())

		val modifiedDf = applyFunctions(df, functionSequence)

		modifiedDf
	}

	def geneateFunctionExpression(functionsMappings: util.ArrayList[FunctionsMapping], columnName: String): String = {
		logger.info(s"Inside geneateFunctionExpression")
		logger.info(s"The function mapping list is ${functionsMappings}")
		logger.info(s"The column Name is ${columnName}")

		var functionString = ""

		for (functionMapping: FunctionsMapping <- functionsMappings.asScala) {
			functionString = functionString +
				s"${functionMapping.function_name}("
		}

		functionString = functionString + s" `${columnName}` "

		//add trailing brackets
		for (index <- 1 to functionsMappings.size()) {
			functionString = functionString + s")"
		}

		val retrunValue = functionString + s" as  `${columnName}`"

		logger.info("The function sequence is " + retrunValue)

		return retrunValue
	}

	def applyFunctions(inputDataFrame: DataFrame, functionSequence: Seq[Column]): DataFrame = {

		val df = inputDataFrame.select(functionSequence: _*)
		df

	}

}
