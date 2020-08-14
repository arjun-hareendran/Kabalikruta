package com.arjun.databricks

import java.sql.DriverManager

import com.databricks.dbutils_v1.{DBUtilsHolder, DBUtilsV1}


object DbutilsInterface {


	def getConnetion(): Unit = {

		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		type DBUtils = DBUtilsV1
		val dbutils: DBUtils = DBUtilsHolder.dbutils
		val username = dbutils.secrets.get(scope = "key-vault-secrets-mssql", key = "Databrics-SQL-UserName-AHAREEN")
		val password = dbutils.secrets.get(scope = "key-vault-secrets-mssql", key = "Databrics-SQL-Password-AHAREEN")
		val connectionStr = ""
		val connection = DriverManager.getConnection(connectionStr)
		val statement = connection.createStatement();
		val selectSql = "SELECT top 1 * FROM dq_automation limit ";
		val resultSet = statement.executeQuery(selectSql);

		while (resultSet.next()) {
			println(resultSet.getString(1));
		}

	}
}
