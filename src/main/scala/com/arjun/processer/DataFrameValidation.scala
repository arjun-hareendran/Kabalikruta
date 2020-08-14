package com.arjun.processer

import java.util

import com.arjun.datastructures.{TargetColumnValidation, TargetTableColumnDefinition, TargetTableDefinition}
import com.arjun.functions.Validations
import com.arjun.metadataframework.MetadataHandler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class DataFrameValidation(metadataHandler: MetadataHandler,
													targetTableDefinition: TargetTableDefinition
													, targetTableColumnDefinitions: util.ArrayList[TargetTableColumnDefinition]
													, sparkSession: SparkSession
												 ) {


	val logger = LoggerFactory.getLogger(classOf[DataFrameValidation])
	val functions = new Validations()

	def validate(inputDataFrame: DataFrame): DataFrame = {

		logger.info("Inside validate")

		var validatedDf = inputDataFrame
		for (targetTableColumnDefinition <- targetTableColumnDefinitions.asScala) {

			validatedDf = validateRecords(validatedDf, targetTableColumnDefinition)
		}

		logger.info("Validation completed ")

		validatedDf
	}


	def validateRecords(inputDataFrame: DataFrame, targetTableColumnDefinition: TargetTableColumnDefinition): DataFrame = {

		logger.info("Inside validateRecords")

		val columnName = targetTableColumnDefinition.column_name
		val targetColumnValidations: util.ArrayList[TargetColumnValidation]
		= metadataHandler.readTargetColumnValidation(targetTableColumnDefinition.table_column_id)

		var intermediateGood: DataFrame = inputDataFrame
		var tempDataframe: DataFrame = inputDataFrame

		if ( !targetColumnValidations.isEmpty ) {
			for (targetColumnValidation: TargetColumnValidation <- targetColumnValidations.asScala) {

				intermediateGood = targetColumnValidation.validation.toLowerCase() match {
					case "null-check" => functions.performNullCheck(intermediateGood, columnName)
					case "quantity-check" => functions.performQuantityCheck(intermediateGood, columnName)
					case _ => intermediateGood
				}
			}
		}

		intermediateGood
	}
}
