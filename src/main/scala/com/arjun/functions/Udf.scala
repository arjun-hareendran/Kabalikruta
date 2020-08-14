package com.arjun.functions

import java.text.SimpleDateFormat

import org.apache.spark.sql.Column

trait Udf {

	def formattime(timeString: String): String = {

		val displayFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		val parseFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss a");

		displayFormat.setLenient(false)
		parseFormat.setLenient(false)

		try {
			val parsedDate = parseFormat.parse(timeString);
			val retrunval = displayFormat.format(parsedDate);
			return retrunval
		}
		catch {
			case ex: Exception => null
		}

	}

}
