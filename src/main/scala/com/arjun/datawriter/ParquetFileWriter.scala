package com.arjun.datawriter

import java.util.Date

import com.arjun.batchframework._
import com.arjun.datastructures.{SourceExtractDefinition, TargetTableDefinition}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

class ParquetFileWriter(sparkSession: SparkSession) {

	val logger = LoggerFactory.getLogger(classOf[ParquetFileWriter])

	def write(sourceExtractDefinition: SourceExtractDefinition
						, targetTableDefinition: TargetTableDefinition
						, inputDataFrame: DataFrame, region: String ) = {

		logger.info("write")
		try {

			val partitionFetcher = new ParitionFetcher(targetTableDefinition)
			val folderStructre = partitionFetcher.getParition()


			logger.info(s"database name ${region}_${targetTableDefinition.database_name}.${targetTableDefinition.table_name}}")
			logger.info(s"path is ${targetTableDefinition.table_location}${folderStructre}")


			val mode = sourceExtractDefinition.extract_load_type match {
				case "incremental" => "append"
				case "full-load" => "overwrite"
				case _ => throw new Exception("Invalid write mode ")
			}

			val tableCheck: Boolean = sparkSession
				.catalog
				.tableExists(s"${region}_${targetTableDefinition.database_name}.${targetTableDefinition.table_name}")

			logger.info(s"table Check  ${tableCheck}")

			if ( tableCheck ) {
				inputDataFrame.write
					.format("parquet")
					.mode(mode)
					.insertInto(s"`${region}_${targetTableDefinition.database_name}`.`${targetTableDefinition.table_name}`")
			} else {
				inputDataFrame.write
					.format("parquet")
					.option("path", s"${targetTableDefinition.table_location}/${folderStructre}")
					.saveAsTable(s"`${region}_${targetTableDefinition.database_name}`.`${targetTableDefinition.table_name}`")
			}

			logger.info("data write complete")

		} catch {
			case e: Exception => {
				logger.info("Error occured while writing the data to storage ");
				throw new Exception("An Error occured while writing the data")
			}
		}


	}


}
