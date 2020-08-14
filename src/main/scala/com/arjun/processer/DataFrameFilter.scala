package com.arjun.processer

import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

class DataFrameFilter {

	val logger = LoggerFactory.getLogger(classOf[DataFrameFilter])

	def Filter(inputDataframe: DataFrame, filterCondition: String): DataFrame = {

		logger.info("Inside Filter ")

		if ( filterCondition.trim == "" ) {
			return inputDataframe
		} else {
			return inputDataframe.filter(filterCondition)
		}


	}

}
