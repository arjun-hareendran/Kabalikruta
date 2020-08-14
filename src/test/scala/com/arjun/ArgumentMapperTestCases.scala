package com.arjun

import com.arjun.datastructures.ArgumentHolder
import com.arjun.processer.ArgumentMapper
import org.junit.Assert.assertEquals
import org.junit.Test

class ArgumentMapperTestCases {

  @Test
  def testmapCommandLineArguments(): Unit = {

    val inputArguments = Array("prod" ,"zeus-1" ,"2020_5_18")
    var expectedResult = new ArgumentHolder("prod" ,"zeus-1","2020_5_18")

    val argumentMapper =new ArgumentMapper()
    var actualresult = argumentMapper.mapCommandLineArguments(inputArguments)

    assertEquals(expectedResult,actualresult)

    expectedResult = new ArgumentHolder("prod" ,"zeus-2" ,"2020_5_18")
    actualresult = argumentMapper.mapCommandLineArguments(inputArguments)

    assert(expectedResult!= actualresult)



  }

}
