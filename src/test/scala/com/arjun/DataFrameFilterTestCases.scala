package com.arjun

import com.arjun.UnitTestMockClasses.TestMetadataHandler
import com.arjun.processer.DataFrameFilter
import org.apache.spark.sql.SparkSession
import org.junit.Test

class DataFrameFilterTestCases {

	@Test
	def testColumnFilter(): Unit = {

		//data Preparation
		val sparkSession = SparkSession
			.builder
			.master("local")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		import sparkSession.implicits._
		val df = List(
			("name-1", 10),
			("name-2", 20),
			("name-3", 30),
			("name-4", 40),
			("name-5", 50),
			("name-6", 60),
			("name-7", 70),
			("name-8", 80)
		).toDF("firstname", "yearsold")

		val metadataHanlder = new TestMetadataHandler(null, null)
		val targetColumnFilter = metadataHanlder.readTargetColumnFilterTable("zeus-ira-table", "dev_zeus")
		val actualResult = new DataFrameFilter().Filter(df, targetColumnFilter.filter_condition)

		import sparkSession.implicits._
		val expectedResult = List(
			("name-4", 40),
			("name-6", 60)
		).toDF("firstname", "yearsold")

		assert(actualResult.except(expectedResult).count().longValue() == 0)


	}

	@Test
	def testColumnFilterWithNoCondition(): Unit = {

		//data Preparation
		val sparkSession = SparkSession
			.builder
			.master("local")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		import sparkSession.implicits._
		val df = List(
			("name-1", 10),
			("name-2", 20),
			("name-3", 30),
			("name-4", 40),
			("name-5", 50),
			("name-6", 60),
			("name-7", 70),
			("name-8", 80)
		).toDF("firstname", "yearsold")

		val actualResult = new DataFrameFilter().Filter(df, "")

		import sparkSession.implicits._
		val expectedResult = List(
			("name-1", 10),
			("name-2", 20),
			("name-3", 30),
			("name-4", 40),
			("name-5", 50),
			("name-6", 60),
			("name-7", 70),
			("name-8", 80)
		).toDF("firstname", "yearsold")

		assert(actualResult.except(expectedResult).count().longValue() == 0)


	}

}
