package com.arjun.batchframework

import java.sql.{Connection, ResultSet, Statement}
import java.util.UUID

import com.arjun.datastructures.SourceExtractDefinition
import com.arjun.sqlconnection.SqlConnection
import org.slf4j.LoggerFactory
import com.arjun.datastructures.Constants._

class BatchHandler(statement: Statement,region:String) {


	var uuid: UUID = _
	val logger = LoggerFactory.getLogger(classOf[BatchHandler])


	def readBatchTable(): ResultSet = {

		null
	}


	def readJobTable(): ResultSet = {

		null
	}


	def readStatsTable(): ResultSet = {

		null
	}

	def updateBatchTable(dataSourceId: String, status:String): Unit = {

		val sql =
			s"""
				 |update ${region}_${batchSchemaName}.batch
				 |set
				 | 		batch_status = '${status}'
				 | , record_update_timestamp = CURRENT_TIMESTAMP
				 |where
				 |				batch_id = '${uuid}'
				 |
			""".stripMargin

		logger.info(s"Sql Statement is ${sql}")
		val executionResultFlag = statement.execute(sql)
	}

	def updateJobTable(sourceExtractionDefinition:SourceExtractDefinition , status :String): Unit = {

		val sql =
			s"""
				 |update ${region}_${batchSchemaName}.job
				 |set
				 |		job_status = '${status}'
				 | ,  record_update_timestamp = CURRENT_TIMESTAMP
				 |where
				 |				batch_id = '${uuid}' and
				 |    		job_id = '${sourceExtractionDefinition.extract_id}'
				 |
			""".stripMargin

		logger.info(s"Sql Statement is ${sql}")
		statement.execute(sql)
	}

	def updateStatsTable(): Unit = {


	}

	def insertBatchTable(dataSourceId: String): Unit = {

		logger.info("Inside insertBatchTable")

		uuid = UUID.randomUUID()
		val sql =
			s"""
				 |insert into ${region}_${batchSchemaName}.batch
				 |         (  batch_id, data_source_id , batch_name,batch_status)
				 |values   ( '${uuid}', '${dataSourceId}' , '${dataSourceId}-pipeline', 'started' ) 
				 |
			""".stripMargin

		logger.info(s"Sql Statement is ${sql}")
		val executionResult = statement.execute(sql)
	}

	def insertJobTable(sourceExtractionDefinition:SourceExtractDefinition): Unit = {

		val sql =
			s"""
				 |insert into ${region}_${batchSchemaName}.job
				 |         (  job_id ,batch_id , job_name , job_status)
				 |values   ( '${sourceExtractionDefinition.extract_id}',
				 |						'${uuid}' ,
				 |      			'${sourceExtractionDefinition.extract_id}-ingestion', 'started' )
				 |
			""".stripMargin

		logger.info(s"Sql Statement is ${sql}")
		statement.execute(sql)

	}

	def insertStatsTable(sourceExtractionDefinition:SourceExtractDefinition, key:String,value:String): Unit = {

		val sql =
			s"""
				 |insert into ${region}_${batchSchemaName}.stats
				 |         (  job_id ,batch_id , stats_key , stats_value)
				 |values   ( '${sourceExtractionDefinition.extract_id}', '${uuid}' , '${key}', '${value}' )
				 |
			""".stripMargin

		logger.info(s"Sql Statement is ${sql}")
		statement.execute(sql)



	}

	def insertProcess(key:String, value:String): Unit ={
		val sql =
			s"""
				 |insert into ${region}_${batchSchemaName}.process_log
				 |         (  batch_id , process_key , process_status)
				 |values   ( '${uuid}' , '${key}', '${value}' )
				 |
			""".stripMargin

		logger.info(s"Sql Statement is ${sql}")
		statement.execute(sql)

	}

}
