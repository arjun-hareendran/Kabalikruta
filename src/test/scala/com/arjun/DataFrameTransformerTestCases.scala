package com.arjun

import java.util

import com.arjun.UnitTestMockClasses.TestMetadataHandler
import com.arjun.datastructures.{TargetTableColumnDefinition, TargetTableDefinition}
import com.arjun.processer.DataFrameTransformer
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class DataFrameTransformerTestCases {


	@Test
	def TestTransformDataFunctions(): Unit = {

		//data Preparation
		val sparkSession = SparkSession
			.builder
			.master("local")
			.appName("Kabalikruta")
			.enableHiveSupport()
			.getOrCreate()

		import sparkSession.implicits._
		val df = List(
			("     name-1     ", 100),
			("     name-2     ", 50)
		).toDF("firstname", "yearsold")


		var metadataHanlder = new TestMetadataHandler(null, null)
		var soruceExtractColumnDefinitionArray = metadataHanlder.readSoruceExtractColumnDefinition("zeus-ira-extract")

		val targetTableDefinition: TargetTableDefinition = metadataHanlder
			.readTargetTableDefinitionTable("zeus-ira-extract")

		val targetTableColumnDefinition: util.ArrayList[TargetTableColumnDefinition] = metadataHanlder
			.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)


		var dataTransformer = new DataFrameTransformer(soruceExtractColumnDefinitionArray
			, metadataHanlder
			, targetTableDefinition
			, targetTableColumnDefinition)

		var functionsMappingArray = metadataHanlder.readFunctionMappingTable("zeus-ira-name-tbl-col", "zeus-ira-table")

		var actualResult = dataTransformer.geneateFunctionExpression(functionsMappingArray, "firstname")
		var expectedResult = "ltrim(rtrim( `firstname` )) as  `firstname`"

		assertEquals(expectedResult, actualResult)


		functionsMappingArray = metadataHanlder.readFunctionMappingTable("zeus-ira-age-tbl-col", "zeus-ira-table")
		actualResult = dataTransformer.geneateFunctionExpression(functionsMappingArray, "yearsold")
		expectedResult = "ascii( `yearsold` ) as  `yearsold`"
		assertEquals(actualResult, expectedResult)

		var transformedActualData = dataTransformer.transformData(df)
		var transformedExpectedData = List(
			("name-1", 49),
			("name-2", 53)
		).toDF("firstname", "yearsold")

		transformedActualData.show()
		assert(transformedActualData.except(transformedExpectedData).count().longValue() == 0)

	}

}
