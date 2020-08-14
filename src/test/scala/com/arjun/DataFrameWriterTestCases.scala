package com.arjun

import com.arjun.UnitTestMockClasses.TestMetadataHandler
import com.arjun.datastructures._
import com.arjun.datawriter.{DataWriter, ParitionFetcher}
import org.apache.spark.sql.SparkSession
import org.junit.Test

class DataFrameWriterTestCases {


  @Test
  def testValidateDataFrame(): Unit = {

    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("Kabalikruta")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._
    val df = List(
      ("name-1", 49),
      ("name-2", 53)
    ).toDF("name", "age")


    val metadataHanlder = new TestMetadataHandler(null, null)

    val targetTableDefinition: TargetTableDefinition = metadataHanlder.readTargetTableDefinitionTable("zeus-ira-extract")
    val targetTableColumnDefinition = metadataHanlder.readTargetTableColumnDefinitionTable(targetTableDefinition.table_id)
    val sourceExtractDefinition: SourceExtractDefinition = metadataHanlder.readSourceExtractDefinition("zeus-ira-extract")
    val sourceDefinition: SourceDefinition = metadataHanlder.readSourceDefintionTable(data_source_id = "zeus")
    sparkSession.sqlContext.sql(s"drop table if exists ${targetTableDefinition.database_name}.${targetTableDefinition.table_name}")


    val dataFrameWriter = new DataWriter(targetTableDefinition
      , targetTableColumnDefinition
      , sourceExtractDefinition
      , sourceDefinition,new ArgumentHolder("dev","zeus", "2020_5_18"),sparkSession)
      .writeData(df)


    val partitionFetcher = new ParitionFetcher(targetTableDefinition)
    val folderStructre = partitionFetcher.getParition()

    val actualDf = sparkSession.read.parquet(s"${targetTableDefinition.table_location}/${folderStructre}")
    assert(actualDf.except(df).count().longValue() == 0)


  }
}
