package com.arjun

import java.util

import com.arjun.UnitTestMockClasses.TestMetadataHandler
import com.arjun.datastructures.{SourceExtractDefinition, TargetTableColumnDefinition, TargetTableDefinition}
import com.arjun.processer.DataFrameColumnMapper
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.{Column, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test


class DataFrameColumnMapperTestCases {

	@Test
	def testColumnMapper(): Unit = {

		//data Preparation
		val sparkSession = SparkSession
			.builder
			.master("local")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		import sparkSession.implicits._
		val df = List(
			("name-1", 100),
			("name-2", 50)
		).toDF("firstname", "yearsold")


		val metadataHanlder = new TestMetadataHandler(null, null)
		var sourceExtractDefinition: SourceExtractDefinition = metadataHanlder.readSourceExtractDefinition("zeus-ira-extract")
		var sourceExtractColumnDefinitionArray = metadataHanlder.readSoruceExtractColumnDefinition(sourceExtractDefinition.extract_id)

		val targetTableDefinition: TargetTableDefinition = metadataHanlder
			.readTargetTableDefinitionTable(sourceExtractDefinition.extract_id)

		val targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition] = metadataHanlder
			.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)

		var dataFrameColumnMapper = new DataFrameColumnMapper(sourceExtractDefinition,
			sourceExtractColumnDefinitionArray
			, metadataHanlder
			, targetTableDefinition
			, targetTableColumnDefinition)

		var (actualDataFrame) = dataFrameColumnMapper.map(df)


		val expectedDataframe = List(
			("name-1", 100),
			("name-2", 50)
		).toDF("firstname", "yearsold")

		assert(actualDataFrame.except(expectedDataframe).count().longValue() == 0)

	}


	@Test
	def testPerformMapAndCast(): Unit = {

		//data Preparation
		val sparkSession = SparkSession
			.builder
			.master("local")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		import sparkSession.implicits._
		val df = List(
			("iss-1234", "test issue", "Y"),
			("iss-4567", "test issue", "Y")
		).toDF("issue", "description", "flag")


		val metadataHanlder = new TestMetadataHandler(null, null)
		var sourceExtractDefinition: SourceExtractDefinition = metadataHanlder.readSourceExtractDefinition("zeus-ira-extract")
		var sourceExtractColumnDefinitionArray = metadataHanlder.readSoruceExtractColumnDefinition(sourceExtractDefinition.extract_id)

		val targetTableDefinition: TargetTableDefinition = metadataHanlder
			.readTargetTableDefinitionTable(sourceExtractDefinition.extract_id)

		val targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition] = metadataHanlder
			.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)


		var dataFrameColumnMapper = new DataFrameColumnMapper(sourceExtractDefinition
			, sourceExtractColumnDefinitionArray
			, metadataHanlder
			, targetTableDefinition
			, targetTableColumnDefinition)

		var ColumnMapping: Seq[Column] = Seq.empty[Column]
		ColumnMapping = ColumnMapping :+ expr(s" cast(  `issue` as  string) as `issue` ")
		ColumnMapping = ColumnMapping :+ expr(s" cast(  `description` as  string) as `description` ")
		ColumnMapping = ColumnMapping :+ expr(s" cast(  `flag` as  string) as `flag` ")

		val actualResult = dataFrameColumnMapper.performMapAndCast(df, ColumnMapping)

		val expectedDataframe = List(
			("iss-1234", "test issue", "Y"),
			("iss-4567", "test issue", "Y")
		).toDF("issue", "description", "flag")

		assert(actualResult.except(expectedDataframe).count().longValue() == 0)

	}


	@Test
	def testGenerateTableColumnMappingSelectExpr(): Unit = {

		//data Preparation
		val sparkSession = SparkSession
			.builder
			.master("local")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		import sparkSession.implicits._
		val df = List(
			("iss-1234", "test issue", "Y"),
			("iss-4567", "test issue", "Y")
		).toDF("issue", "description", "flag")


		val metadataHanlder = new TestMetadataHandler(null, null)
		var sourceExtractDefinition: SourceExtractDefinition = metadataHanlder.readSourceExtractDefinition("zeus-ira-extract")
		var sourceExtractColumnDefinitionArray = metadataHanlder.readSoruceExtractColumnDefinition(sourceExtractDefinition.extract_id)

		val targetTableDefinition: TargetTableDefinition = metadataHanlder
			.readTargetTableDefinitionTable(sourceExtractDefinition.extract_id)

		val targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition] = metadataHanlder
			.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)



		var dataFrameColumnMapper = new DataFrameColumnMapper(sourceExtractDefinition
			, sourceExtractColumnDefinitionArray
			, metadataHanlder
			, targetTableDefinition
			,targetTableColumnDefinition)

		val targetTableColumnDefinitionArray = new util.ArrayList[TargetTableColumnDefinition]

		targetTableColumnDefinitionArray.add(new TargetTableColumnDefinition(
			id = 1,
			table_column_id = "issue-column",
			table_id = "test-tab",
			column_name = "issue",
			column_data_type = "STRING",
			column_description = "test Column",
			record_active_flag = "Y",
			record_insert_timestamp = null,
			record_update_timestamp = null
		))

		targetTableColumnDefinitionArray.add(new TargetTableColumnDefinition(
			id = 2,
			table_column_id = "description-column",
			table_id = "test-tab",
			column_name = "description",
			column_data_type = "STRING",
			column_description = "test Column",
			record_active_flag = "Y",
			record_insert_timestamp = null,
			record_update_timestamp = null
		))

		val actualExpr = dataFrameColumnMapper.generateTableColumnMappingSelectExpr(targetTableColumnDefinitionArray)


		var ColumnMapping: Seq[Column] = Seq.empty[Column]
		ColumnMapping = ColumnMapping :+ expr(s" `issue` as `issue` ")
		ColumnMapping = ColumnMapping :+ expr(s" `description` as `description` ")

		assertEquals(ColumnMapping.toString(), actualExpr.toString())

		val actualResult = dataFrameColumnMapper.performMapAndCast(df, actualExpr)

		val expectedDataframe = List(
			("iss-1234", "test issue"),
			("iss-4567", "test issue")
		).toDF("issue", "description")

		assert(actualResult.except(expectedDataframe).count().longValue() == 0)


	}
}


