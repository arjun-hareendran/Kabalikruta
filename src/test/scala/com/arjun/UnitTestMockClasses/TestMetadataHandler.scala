package com.arjun.UnitTestMockClasses

import java.sql.Statement
import java.util

import com.arjun.datastructures.{TargetColumnValidation, TargetTableColumnDefinition, _}
import com.arjun.metadataframework.MetadataHandler

class TestMetadataHandler(sqlStatement: Statement, inputArguments: ArgumentHolder)
	extends MetadataHandler(sqlStatement, inputArguments) {

	override def readFunctionMappingTable(TableColumnId: String, Tableid: String): util.ArrayList[FunctionsMapping] = {


		logger.info("Inside readFunctionMappingTable")
		val functionsMappingArray: util.ArrayList[FunctionsMapping] = new util.ArrayList[FunctionsMapping]

		if ( TableColumnId.equalsIgnoreCase("zeus-ira-name-tbl-col") && Tableid.equalsIgnoreCase("zeus-ira-table") ) {
			functionsMappingArray.add(
				new FunctionsMapping(

					table_column_id = "zeus-ira-name-tbl-col",
					table_id = "zeus-ira-table",
					mapping_sequence_id = 1,
					function_type = "inbuilt",
					function_name = "ltrim",
					function_parameter = "",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null

				))

			functionsMappingArray.add(
				new FunctionsMapping(
					table_column_id = "zeus-ira-name-tbl-col",
					table_id = "zeus-ira-table",
					mapping_sequence_id = 2,
					function_type = "inbuilt",
					function_name = "rtrim",
					function_parameter = "",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null

				))
			logger.info(s"returning value ${functionsMappingArray}")
			return functionsMappingArray
		} else if ( TableColumnId.equalsIgnoreCase("zeus-ira-age-tbl-col") && Tableid.equalsIgnoreCase("zeus-ira-table") ) {

			functionsMappingArray.add(
				new FunctionsMapping(
					table_column_id = "zeus-ira-age-tbl-col",
					table_id = "zeus-ira-table",
					mapping_sequence_id = 1,
					function_type = "inbuilt",
					function_name = "ascii",
					function_parameter = "",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null
				))

			logger.info(s"returning value ${functionsMappingArray}")
			return functionsMappingArray
		}
		else {
			logger.info("returning null")
			return null
		}


	}

	override def readSoruceExtractColumnDefinition(source_extract_id: String): util.ArrayList[SourceExtractColumnDefinition] = {

		logger.info("Inside readSoruceExtractColumnDefinition")

		val soruceExtractColumnDefinitionArray: util.ArrayList[SourceExtractColumnDefinition] = new util.ArrayList[SourceExtractColumnDefinition]

		if ( source_extract_id.equalsIgnoreCase("zeus-ira-extract") ) {
			soruceExtractColumnDefinitionArray.add(
				new SourceExtractColumnDefinition(
					extract_column_id = "zeus-ira-name-ext-col",
					extract_id = "zeus-ira",
					column_name = "firstname",
					column_type = "string",
					column_position = 1,
					column_start_position = 0,
					column_end_position = 0,
					column_description = "Test Column for Junit Testing",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null
				))

			soruceExtractColumnDefinitionArray.add(
				new SourceExtractColumnDefinition(
					extract_column_id = "zeus-ira-age-ext-col",
					extract_id = "zeus-ira",
					column_name = "yearsold",
					column_type = "string",
					column_position = 1,
					column_start_position = 0,
					column_end_position = 0,
					column_description = "Test Column for Junit Testing",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null
				))
			logger.info(s"Retuning value ${soruceExtractColumnDefinitionArray}")
			return soruceExtractColumnDefinitionArray
		} else {
			logger.info("Retuning null ")
			return null
		}


	}

	@Override def readSourceExtractDefinition(source_extract_id: String): SourceExtractDefinition = {

		logger.info("Inside readSourceExtractDefinition")

		if ( source_extract_id == "zeus-ira-extract" ) {
			val sourceExtractDefinition = new SourceExtractDefinition(
				extract_id = "zeus-ira-extract",
				data_source_id = "zeus ",
				extract_name = "ira",
				extract_type = "csv",
				extract_location = "",
				extract_load_type = "incremental",
				extract_frequency = "daily",
				extract_description = "description",
				record_active_flag = "Y",
				record_insert_timestamp = null,
				record_update_timestamp = null
			)
			logger.info(s"Retuning value ${sourceExtractDefinition}")
			return sourceExtractDefinition
		} else {
			logger.info("returning null")
			return null
		}
	}

	override def readSourceTargetColumnMappingTable(table_id: String, extract_column_id: String): SourceTargetColumnMapping = {

		logger.info("Inside readSourceTargetColumnMappingTable")

		if ( extract_column_id == "zeus-ira-name-ext-col" && table_id == "zeus-ira-table" ) {

			val sourceTargetColumnMapping = new SourceTargetColumnMapping(

				table_column_id = "zeus-ira-name-tbl-col"
				, table_id = "zeus-ira-table"
				, extract_id = "zeus-ira-extract"
				, extract_column_id = "zeus-ira-name-ext-col"
				, record_active_flag = "Y"
				, record_insert_timestamp = null
				, record_update_timestamp = null
			)
			logger.info(s"Retuning value ${sourceTargetColumnMapping}")
			return sourceTargetColumnMapping

		} else if ( extract_column_id == "zeus-ira-age-ext-col" && table_id == "zeus-ira-table" ) {

			val sourceTargetColumnMapping = new SourceTargetColumnMapping(
				table_column_id = "zeus-ira-age-tbl-col"
				, table_id = "zeus-ira-table"
				, extract_id = "zeus-ira-extract"
				, extract_column_id = "zeus-ira-age-ext-col"
				, record_active_flag = "Y"
				, record_insert_timestamp = null
				, record_update_timestamp = null
			)
			logger.info(s"Retuning value ${sourceTargetColumnMapping}")
			return sourceTargetColumnMapping
		} else {
			logger.info("retuning null")
			return null
		}
	}

	override def readTargetTableDefinitionTable(extract_id: String): TargetTableDefinition = {


		logger.info("Inside readTargetTableDefinitionTable")

		if ( extract_id == "zeus-ira-extract" ) {

			val targetTableDefinition = new TargetTableDefinition(
				table_id = "zeus-ira-table"
				, data_source_id = "zeus"
				, extract_id = "zeus-ira-extract"
				, table_name = "ira_zeus"
				, database_name = "dev_zeus"
				, table_format = "parquet"
				, compression = "snappy"
				, table_location = "src/main/resource/output"
				, table_description = "description"
				, partition = "yyyy-MM-dd"
				, record_active_flag = "Y"
				, record_insert_timestamp = null
				, record_update_timestamp = null
			)
			logger.info(s"Retuning ${targetTableDefinition}")
			return targetTableDefinition
		}
		else {
			logger.info("Returning null")

			return null
		}

	}

	override def readTargetTableColumnDefinitionTable(table_id: String): util.ArrayList[TargetTableColumnDefinition] = {

		logger.info("Inside readTargetTableColumnDefinitionTable")

		val targetTableColumnDefinitionArray: util.ArrayList[TargetTableColumnDefinition] = new util.ArrayList[TargetTableColumnDefinition]

		if ( table_id == "zeus-ira-table" ) {
			targetTableColumnDefinitionArray.add(
				new TargetTableColumnDefinition(
					id = 1,
					table_column_id = "zeus-ira-name-tbl-col",
					table_id = "zeus-ira-table",
					column_name = "firstname",
					column_data_type = "string",
					column_description = "first name ",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null
				))

			targetTableColumnDefinitionArray.add(
				new TargetTableColumnDefinition(
					id = 2,
					table_column_id = "zeus-ira-age-tbl-col",
					table_id = "zeus-ira-table",
					column_name = "yearsold",
					column_data_type = "string",
					column_description = "Age",
					record_active_flag = "Y",
					record_insert_timestamp = null,
					record_update_timestamp = null
				))

			logger.info(s"returning value ${targetTableColumnDefinitionArray}")
			return targetTableColumnDefinitionArray
		} else {
			logger.info("Returning null")
			return null
		}
	}

	override def readTargetColumnValidation(target_table_column_definition_id: String): util.ArrayList[TargetColumnValidation] = {

		logger.info("Inside readTargetColumnValidation")

		val targetColumnValidationArray: util.ArrayList[TargetColumnValidation] = new util.ArrayList[TargetColumnValidation]

		if ( target_table_column_definition_id == "zeus-ira-name-tbl-col" ) {
			targetColumnValidationArray.add(
				new TargetColumnValidation(
					table_column_id = "zeus-ira-name-tbl-col"
					, validation_sequence = "vs1"
					, validation = "null-check"
					, process_abort_on_validation_failure = "N"
					, record_active_flag = "Y"
					, record_insert_timestamp = null
					, record_update_timestamp = null
				))

			targetColumnValidationArray.add(
				new TargetColumnValidation(
					table_column_id = "zeus-ira-name-tbl-col"
					, validation_sequence = "vs2"
					, validation = "vin-check"
					, process_abort_on_validation_failure = "N"
					, record_active_flag = "Y"
					, record_insert_timestamp = null
					, record_update_timestamp = null
				))


			logger.info(s"returning value ${targetColumnValidationArray}")
			return targetColumnValidationArray
		} else if ( target_table_column_definition_id == "zeus-ira-age-tbl-col" ) {
			targetColumnValidationArray.add(
				new TargetColumnValidation(
					table_column_id = "zeus-ira-age-tbl-col"
					, validation_sequence = "vs1"
					, validation = "null-check"
					, process_abort_on_validation_failure = "N"
					, record_active_flag = "Y"
					, record_insert_timestamp = null
					, record_update_timestamp = null
				))


			logger.info(s"returning value ${targetColumnValidationArray}")
			return targetColumnValidationArray
		}
		else {
			logger.info("Returning empty")
			return new util.ArrayList[TargetColumnValidation]
		}
	}

	override def readSourceDefintionTable(data_source_id: String): SourceDefinition = {
		logger.info("Inside readTargetTableColumnDefinitionTable")

		val targetTableColumnDefinitionArray: util.ArrayList[TargetTableColumnDefinition] = new util.ArrayList[TargetTableColumnDefinition]

		if ( data_source_id == "zeus" ) {

			val soruceDefinition = new SourceDefinition(

				data_source_id = "zeus"
				, data_source_version = "1.0"
				, data_source_description = "Quality tracker"
				, contact_person_email_id = "arjun.hareendran@personal.com"
				, record_active_flag = "Y"
				, record_insert_timestamp = null
				, record_update_timestamp = null)
			logger.info(s"returning value ${soruceDefinition}")
			return soruceDefinition
		} else {
			logger.info("Returning null")
			return null
		}


	}

	override def readTargetColumnFilterTable(table_id: String, database_name: String): TargetColumnFilter = {

		logger.info("Inside readTargetTableColumnDefinitionTable")

		if ( database_name == "dev_zeus" && table_id == "zeus-ira-table" ) {

			val targetColumnFilter = new TargetColumnFilter(

				table_id = "zeus-ira-table"
				, database_name = "dev_zeus"
				, filter_condition = "(yearsold > 30 and yearsold < 70) and (firstname != 'name-5')"
				, record_active_flag = "arjun.hareendran@personal.com"
				, record_insert_timestamp = null
				, record_update_timestamp = null)

			logger.info(s"returning value ${targetColumnFilter}")
			return targetColumnFilter
		} else {
			logger.info("Returning null")
			return null
		}


	}


}
