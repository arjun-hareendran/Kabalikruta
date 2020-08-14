package com.arjun

import com.arjun.validation.InputArgumentChecker
import org.junit.Assert.assertEquals
import org.junit.Test

class InputArgumentCheckerTestCases {


  @Test
  def testCheckArguments(): Unit = {

    var arguments = Array("dev" ,"zeus" ,"zeus")
    val inputArgumentChecker = new InputArgumentChecker()

    var actualResult = inputArgumentChecker.checkArguments(arguments)
    var expectedResult = true
    assertEquals(expectedResult ,actualResult)

    arguments = Array()

    actualResult = inputArgumentChecker.checkArguments(arguments)
    expectedResult = false

    assertEquals(expectedResult ,actualResult)


    arguments = null

    actualResult = inputArgumentChecker.checkArguments(arguments)
    expectedResult = false

    assertEquals(expectedResult ,actualResult)


  }
}
