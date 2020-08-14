package com.arjun


import com.arjun.driver.Orchestrator
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.Test

class OrchestratorTestCases {

	// To do
	//Need better test cases

	@Test
	def testOrchestration(): Unit = {

		val expectedMessage = "Invalid Input Arguments"
		val orchestrator = new Orchestrator()

		try {
			orchestrator.process(null)
		} catch {
			case actualException: Exception => assertEquals(expectedMessage, actualException.getMessage)
		}

		val actualResult = orchestrator.processArguments(Array("dev", "zeus", "2020_5_18"))
		assertEquals(true, actualResult)
		assertEquals("dev", orchestrator.argumentHolder.region)
		assertEquals("zeus", orchestrator.argumentHolder.sourceId)


	}
}
