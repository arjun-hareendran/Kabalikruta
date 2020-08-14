package com.arjun

import com.arjun.UnitTestMockClasses.TestMetadataHandler
import com.arjun.datastructures.TargetTableDefinition
import com.arjun.processer.DataFrameValidation
import org.apache.spark.sql.SparkSession
import org.junit.Test

class DataFrameValidationTestCases {


  @Test
  def testValidateDataFrame(): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("Kabalikruta")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._
    val df = Seq(
      (Some("WBC123"), Some(100))
      , (Some("WBC12345678912345"), Some(100))
      , (None, Some(100))
      , (Some("WBC12345678911010"), None)
      , (None, None)
    ).toDF("firstname", "yearsold")


    val metadataHanlder = new TestMetadataHandler(null, null)

    val targetTableDefinition: TargetTableDefinition = metadataHanlder.readTargetTableDefinitionTable("zeus-ira-extract")
    val targetTableColumnDefinition = metadataHanlder.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)

    println(targetTableDefinition)
    println(targetTableColumnDefinition)

    val dataframeValidation = new DataFrameValidation(metadataHandler = metadataHanlder,
      targetTableDefinition = targetTableDefinition,
      targetTableColumnDefinitions = targetTableColumnDefinition,
      sparkSession = sparkSession
    )

    val actualvalidateDataFrame = dataframeValidation.validate(df)

    val expectedvalidateDataFrame = List(
      ("WBC12345678912345", 100)
    ).toDF("firstname", "yearsold")
    assert(actualvalidateDataFrame.except(expectedvalidateDataFrame).count().longValue() == 0)

    actualvalidateDataFrame.show()

  }
}
