package com.arjun.metadataframework

import java.sql.{ResultSet, Statement}
import java.util

import com.arjun.datastructures.Constants._
import com.arjun.datastructures._
import org.slf4j.LoggerFactory

class MetadataHandler(sqlStatement: Statement, inputArguments: ArgumentHolder) {

	val logger = LoggerFactory.getLogger(classOf[MetadataHandler])

	def readSourceDefintionTable(data_source_id: String): SourceDefinition = {

		logger.info("Inside readSourceDefintionTable")


		val sql =
			s"""
				 |select
				 |     data_source_id
				 |    ,data_soruce_version
				 |    ,data_soruce_description
				 |    ,contact_person
				 |    ,record_active_flag
				 |    ,record_insert_timestamp
				 |    ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.source_definition
				 |where
				 |      data_source_id = '${data_source_id}'
				 |      and record_active_flag = 'Y'
				 |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)

		if ( !resultSet.isBeforeFirst() ) {
			logger.info("Exception Occurred as the result from the metadata table is empty")
			throw new Exception("Empty Result from table")
		} else {

			logger.info("reading data ")
			resultSet.next()

			val returnValue = new SourceDefinition(
				data_source_id = resultSet.getString("data_source_id"),
				data_source_version = resultSet.getString("data_soruce_version"),
				data_source_description = resultSet.getString("data_soruce_description"),
				contact_person_email_id = resultSet.getString("contact_person"),
				record_active_flag = resultSet.getString("record_active_flag"),
				record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
				record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
			)

			if ( resultSet.next() ) {
				logger.info("Multiple values found")
				throw new Exception("Multiple entries found")
			}

			logger.info(returnValue.toString)

			return returnValue
		}
	}

	def readSourceExtractDefinitionTable(data_source_id: String): util.ArrayList[SourceExtractDefinition] = {

		logger.info("Inside readSourceExtractDefinitionTable")

		val sql =
			s"""
				 |select
				 |  extract_id
				 | ,data_source_id
				 | ,extract_name
				 | ,extract_type
				 | ,extract_location
				 | ,extract_load_type
				 | ,extract_frequency
				 | ,extract_description
				 | ,record_active_flag
				 | ,record_insert_timestamp
				 | ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.source_extract_definition
				 |where
				 |      data_source_id = '${data_source_id}'
				 |      and record_active_flag = 'Y'
				 |
         |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)

		if ( !resultSet.isBeforeFirst() ) {
			throw new Exception("No records in source_extract_definition")
		} else {

			val SourceExtractDefinitionArray = new util.ArrayList[SourceExtractDefinition]

			while (resultSet.next()) {
				SourceExtractDefinitionArray.add(
					new SourceExtractDefinition(
						extract_id = resultSet.getString("extract_id"),
						data_source_id = resultSet.getString("data_source_id"),
						extract_name = resultSet.getString("extract_name"),
						extract_type = resultSet.getString("extract_type"),
						extract_location = resultSet.getString("extract_location"),
						extract_load_type = resultSet.getString("extract_load_type"),
						extract_frequency = resultSet.getString("extract_frequency"),
						extract_description = resultSet.getString("extract_description"),
						record_active_flag = resultSet.getString("record_active_flag"),
						record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
						record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
					)
				)
			}

			logger.info(SourceExtractDefinitionArray.toString)
			return SourceExtractDefinitionArray
		}
	}

	def readTargetTableDefinitionTable(extract_id: String): TargetTableDefinition = {

		logger.info("Inside readTargetTableDefinitionTable")

		val sql =
			s"""
				 |select
				 |    table_id
				 |   ,data_source_id
				 |   ,extract_id
				 |   ,table_name
				 |   ,database_name
				 |   ,table_format
				 |   ,compression
				 |   ,table_location
				 |   ,table_description
				 |   ,partition_format
				 |   ,record_active_flag
				 |   ,record_insert_timestamp
				 |   ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.target_table_definition
				 |where
				 |       extract_id = '${extract_id}'
				 |       and record_active_flag = 'Y'
				 |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)

		if ( !resultSet.isBeforeFirst() ) {
			throw new Exception("No entries found in metadata table")
		} else {

			// read the first record.
			resultSet.next()

			val retrunValue = new TargetTableDefinition(
				table_id = resultSet.getString("table_id"),
				data_source_id = resultSet.getString("data_source_id"),
				extract_id = resultSet.getString("extract_id"),
				table_name = resultSet.getString("table_name"),
				database_name = resultSet.getString("database_name"),
				table_format = resultSet.getString("table_format"),
				compression = resultSet.getString("compression"),
				table_location = resultSet.getString("table_location"),
				table_description = resultSet.getString("table_description"),
				partition = resultSet.getString("partition_format"),
				record_active_flag = resultSet.getString("record_active_flag"),
				record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
				record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
			)

			if ( resultSet.next() ) {

				throw new Exception("Multiple records found")
			}

			logger.info(retrunValue.toString)

			return retrunValue
		}
	}

	def readTargetTableColumnDefinitionTable(table_id: String): util.ArrayList[TargetTableColumnDefinition] = {

		logger.info("Inside readTargetTableColumnDefinitionTable")

		val sql =
			s"""
				 |select
				 |			 id
				 |      ,table_column_id
				 |      ,table_id
				 |      ,column_name
				 |      ,column_data_type
				 |      ,column_description
				 |      ,record_active_flag
				 |      ,record_insert_timestamp
				 |      ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.target_table_column_definition
				 |
         |where
				 |      table_id = '${table_id}'
				 |      and record_active_flag = 'Y'
				 |
				 | order by id
				 |
         |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)


		if ( !resultSet.isBeforeFirst() ) {
			throw new Exception("No records in target_table_definition")
		} else {

			val TargetTableColumnDefinitionArray = new util.ArrayList[TargetTableColumnDefinition]

			while (resultSet.next()) {
				TargetTableColumnDefinitionArray.add(
					new TargetTableColumnDefinition(
						id = resultSet.getInt("id"),
						table_column_id = resultSet.getString("table_column_id"),
						table_id = resultSet.getString("table_id"),
						column_name = resultSet.getString("column_name"),
						column_data_type = resultSet.getString("column_data_type"),
						column_description = resultSet.getString("column_description"),
						record_active_flag = resultSet.getString("record_active_flag"),
						record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
						record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
					)
				)
			}

			logger.info(TargetTableColumnDefinitionArray.toString)
			return TargetTableColumnDefinitionArray
		}


	}

	def readSourceTargetColumnMappingTable(tableId: String, tableColumnId: String): SourceTargetColumnMapping = {

		logger.info("Inside readSourceTargetColumnMappingTable")

		val sql =
			s"""
				 |select
				 |      table_column_id
				 |     ,table_id
				 |     ,extract_id
				 |     ,extract_column_id
				 |     ,record_active_flag
				 |     ,record_insert_timestamp
				 |     ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.source_target_column_mapping
				 |where
				 |      table_column_id = '${tableColumnId}'
				 |      and table_id = '${tableId}'
				 |		  and record_active_flag = 'Y'
				 |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)

		if ( !resultSet.isBeforeFirst() ) {
			logger.info("No Mapping record found")
			return null
		} else {

			resultSet.next()

			val retrunvalue = new SourceTargetColumnMapping(
				table_column_id = resultSet.getString("table_column_id"),
				table_id = resultSet.getString("table_id"),
				extract_id = resultSet.getString("extract_id"),
				extract_column_id = resultSet.getString("extract_column_id"),
				record_active_flag = resultSet.getString("record_active_flag"),
				record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
				record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
			)

			logger.info(retrunvalue.toString)
			return retrunvalue
		}
	}

	def readFunctionMappingTable(TableColumnId: String, TableId: String): util.ArrayList[FunctionsMapping] = {

		logger.info("Inside readFunctionMappingTable")

		val sql =
			s"""
				 |select
				 |   table_column_id
				 | , table_id
				 | , mapping_sequence_id
				 | , function_type
				 | , function_name
				 | , function_parameter
				 | , record_active_flag
				 | , record_insert_timestamp
				 | , record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.function_mapping
				 |
         | where table_column_id = '${TableColumnId}'
				 | 				and  table_id = '${TableId}'
				 |        and record_active_flag = 'Y'
				 |
         | order by mapping_sequence_id desc
				 |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)


		if ( !resultSet.isBeforeFirst() ) {
			logger.info("No functions found for the column")
			return null
		} else {

			val FunctionsMappingArray = new util.ArrayList[FunctionsMapping]

			while (resultSet.next()) {
				FunctionsMappingArray.add(
					new FunctionsMapping(
						table_column_id = resultSet.getString("table_column_id"),
						table_id = resultSet.getString("table_id"),
						mapping_sequence_id = resultSet.getInt("mapping_sequence_id"),
						function_type = resultSet.getString("function_type"),
						function_name = resultSet.getString("function_name"),
						function_parameter = resultSet.getString("function_parameter"),
						record_active_flag = resultSet.getString("record_active_flag"),
						record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
						record_update_timestamp = resultSet.getTimestamp("record_update_timestamp"))
				)
			}
			return FunctionsMappingArray
		}
	}

	def readSoruceExtractColumnDefinition(source_extract_id: String): util.ArrayList[SourceExtractColumnDefinition] = {

		logger.info("Inside readSoruceExtractColumnDefinition")

		val sql =
			s"""
				 |select
				 |    extract_column_id
				 |   ,extract_id
				 |   ,column_name
				 |   ,column_type
				 |   ,column_position
				 |   ,column_start_position
				 |   ,column_end_position
				 |   ,column_description
				 |   ,record_active_flag
				 |   ,record_insert_timestamp
				 |   ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.soruce_extract_column_definition
				 |where
				 |    extract_id = '${source_extract_id}'
				 |     and record_active_flag = 'Y'
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)


		if ( !resultSet.isBeforeFirst() ) {
			throw new Exception("No records in soruce_extract_column_definition")
		} else {

			val soruceExtractColumnDefinitionArray = new util.ArrayList[SourceExtractColumnDefinition]

			while (resultSet.next()) {
				soruceExtractColumnDefinitionArray.add(
					new SourceExtractColumnDefinition(
						extract_column_id = resultSet.getString("extract_column_id"),
						extract_id = resultSet.getString("extract_id"),
						column_name = resultSet.getString("column_name"),
						column_type = resultSet.getString("column_type"),
						column_position = resultSet.getInt("column_position"),
						column_start_position = resultSet.getInt("column_start_position"),
						column_end_position = resultSet.getInt("column_end_position"),
						column_description = resultSet.getString("column_description"),
						record_active_flag = resultSet.getString("record_active_flag"),
						record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp"),
						record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
					)
				)
			}

			logger.info(soruceExtractColumnDefinitionArray.toString)

			return soruceExtractColumnDefinitionArray
		}
	}

	def readTargetColumnValidation(table_column_id: String): util.ArrayList[TargetColumnValidation] = {

		logger.info("Inside readTargetColumnValidation")

		val sql =
			s"""
				 |select
				 |      table_column_id
				 |     ,validation_sequence
				 |     ,validation
				 |     ,process_abort_on_validation_failure
				 |     ,record_active_flag
				 |     ,record_insert_timestamp
				 |     ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.target_column_validation
				 |where
				 |          table_column_id = '${table_column_id}'
				 |     and  record_active_flag = 'Y'
				 | order by validation_sequence
				 |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)


		if ( !resultSet.isBeforeFirst() ) {
			logger.info("No validation for the column");
			return new util.ArrayList[TargetColumnValidation]
		} else {

			val targetColumnValidationArray = new util.ArrayList[TargetColumnValidation]

			while (resultSet.next()) {
				targetColumnValidationArray.add(
					new TargetColumnValidation(
						table_column_id = resultSet.getString("table_column_id")
						, validation_sequence = resultSet.getString("validation_sequence")
						, validation = resultSet.getString("validation")
						, process_abort_on_validation_failure = resultSet.getString("process_abort_on_validation_failure")
						, record_active_flag = resultSet.getString("record_active_flag")
						, record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp")
						, record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")


					))
			}
			return targetColumnValidationArray
		}

	}

	def readTargetColumnFilterTable(table_id: String, database_name: String): TargetColumnFilter = {

		logger.info("Inside readTargetColumnFilterTable")

		val sql =
			s"""
				 |select
				 |      table_id
				 |     ,database_name
				 |     ,filter_condition
				 |     ,record_active_flag
				 |     ,record_insert_timestamp
				 |     ,record_update_timestamp
				 |from ${inputArguments.region}_${metadataSchema}.target_column_filter
				 |where
				 |          table_id = '${table_id}'
				 |     and  database_name = '${database_name}'
				 |     and  record_active_flag = 'Y'
				 |
      """.stripMargin

		logger.info("Executing the below Sql Query")
		logger.info(sql)

		val resultSet: ResultSet = sqlStatement.executeQuery(sql)


		if ( !resultSet.isBeforeFirst() ) {
			logger.info("No filterCondition for the column");
			return null
		} else {


			resultSet.next()
			val targetColumnFilter = new TargetColumnFilter(
				table_id = resultSet.getString("table_id")
				, database_name = resultSet.getString("database_name")
				, filter_condition = resultSet.getString("filter_condition")
				, record_active_flag = resultSet.getString("record_active_flag")
				, record_insert_timestamp = resultSet.getTimestamp("record_insert_timestamp")
				, record_update_timestamp = resultSet.getTimestamp("record_update_timestamp")
			)
			return targetColumnFilter
		}

	}

}
