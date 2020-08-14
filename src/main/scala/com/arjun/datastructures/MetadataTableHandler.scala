package com.arjun.datastructures

import java.sql.Timestamp

case class SourceDefinition(
														 data_source_id: String,
														 data_source_version: String,
														 data_source_description: String,
														 contact_person_email_id: String,
														 record_active_flag: String,
														 record_insert_timestamp: Timestamp,
														 record_update_timestamp: Timestamp
													 )


case class SourceExtractDefinition(
																		extract_id: String,
																		data_source_id: String,
																		extract_name: String,
																		extract_type: String,
																		extract_location: String,
																		extract_load_type: String,
																		extract_frequency: String,
																		extract_description: String,
																		record_active_flag: String,
																		record_insert_timestamp: Timestamp,
																		record_update_timestamp: Timestamp
																	)

case class TargetTableDefinition(
																	table_id: String,
																	data_source_id: String,
																	extract_id: String,
																	table_name: String,
																	database_name: String,
																	table_format: String,
																	compression: String,
																	table_location: String,
																	table_description: String,
																	partition: String,
																	record_active_flag: String,
																	record_insert_timestamp: Timestamp,
																	record_update_timestamp: Timestamp

																)

case class SourceExtractColumnDefinition(
																					extract_column_id: String,
																					extract_id: String,
																					column_name: String,
																					column_type: String,
																					column_position: Int,
																					column_start_position: Int,
																					column_end_position: Int,
																					column_description: String,
																					record_active_flag: String,
																					record_insert_timestamp: Timestamp,
																					record_update_timestamp: Timestamp
																				)

case class TargetTableColumnDefinition(
																				id: Int,
																				table_id: String,
																				table_column_id: String,
																				column_name: String,
																				column_data_type: String,
																				column_description: String,
																				record_active_flag: String,
																				record_insert_timestamp: Timestamp,
																				record_update_timestamp: Timestamp

																			)

case class SourceTargetColumnMapping(
																			table_column_id: String,
																			table_id: String,
																			extract_id: String,
																			extract_column_id: String,
																			record_active_flag: String,
																			record_insert_timestamp: Timestamp,
																			record_update_timestamp: Timestamp
																		)

case class TargetColumnValidation(

																	 table_column_id: String,
																	 validation_sequence: String,
																	 validation: String,
																	 process_abort_on_validation_failure: String,
																	 record_active_flag: String,
																	 record_insert_timestamp: Timestamp,
																	 record_update_timestamp: Timestamp

																 )

case class FunctionsMapping(
														 table_column_id: String,
														 table_id: String,
														 mapping_sequence_id: Int,
														 function_type: String,
														 function_name: String,
														 function_parameter: String,
														 record_active_flag: String,
														 record_insert_timestamp: Timestamp,
														 record_update_timestamp: Timestamp
													 )


case class TargetColumnFilter(
															 table_id: String,
															 database_name: String,
															 filter_condition: String,
															 record_active_flag: String,
															 record_insert_timestamp: Timestamp,
															 record_update_timestamp: Timestamp
														 )