package com.arjun.functions

import org.apache.spark.sql.{Column, DataFrame, Row}
import org.slf4j.LoggerFactory

trait BusinessValidations {

	val logger = LoggerFactory.getLogger(classOf[BusinessValidations])

	def performQuantityCheck(inputDataFrame: DataFrame, columnName: String): DataFrame = {

		val badQuantity = (row: Row) => {
			val columnValue = row.getAs(s"${columnName}").toString
			if ( columnValue.trim.length != 5 ) {
				false
			} else {
				true
			}
		}

		val outputDf = inputDataFrame.filter(badQuantity)
		outputDf
	}


}
