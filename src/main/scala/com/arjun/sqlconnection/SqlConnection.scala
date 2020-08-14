package com.arjun.sqlconnection

import java.sql.{Connection, DriverManager, Statement}

import com.databricks.dbutils_v1.{DBUtilsHolder, DBUtilsV1}
import org.slf4j.LoggerFactory

class SqlConnection {

	val logger = LoggerFactory.getLogger(classOf[SqlConnection])
	var connection: Connection = _
	var statement: Statement = _

	def getProdConnection(): Connection = {

		logger.info("Inside getProdConnection ")

		throw new Exception("Production Not configured")
		null
	}


	def getDevConnection(): Connection = {

		logger.info("Inside getDevConnection ")

		type DBUtils = DBUtilsV1
		var connection: Connection = null
		val dbutils: DBUtils = DBUtilsHolder.dbutils
		val username = dbutils.secrets.get(scope = "key-vault-secrets-mssql", key = "Databrics-SQL-UserName-username")
		val password = dbutils.secrets.get(scope = "key-vault-secrets-mssql", key = "Databrics-SQL-Password-password")
		val databasename = ""
		val servernameWithPort =""


		val connectionStr = "jdbc:sqlserver://" +
			servernameWithPort +
			s"database=${databasename};" +
			s"user=${username};" +
			s"password=${password};" +
			"encrypt=true;" +
			"trustServerCertificate=false;" +
			"hostNameInCertificate=*.database.windows.net;" +
			"loginTimeout=30;"

		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			connection = DriverManager.getConnection(connectionStr)
		} catch {
			case e: Exception => logger.error(e.printStackTrace().toString)
		}

		connection
	}

	def connectionRouter(region: String): Connection = {

		logger.info("Inside connectionRouter ")

		if ( region.equalsIgnoreCase("prod") ) {
			connection= getProdConnection()
		} else {
			connection= getDevConnection()
		}
		connection
	}

}
