package com.arjun.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr

trait LogicalValidations {

	def performNullCheck(inputDataFrame: DataFrame, columnName: String): DataFrame = {

		val outputDf = inputDataFrame.where(expr(s"${columnName} is not null"))

		outputDf

	}

}
