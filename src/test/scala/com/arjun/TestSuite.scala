package com.arjun

import org.junit.runner.RunWith
import org.junit.runners.Suite

//Test Case Suite

@RunWith(classOf[Suite])
@Suite.SuiteClasses(Array(classOf[ArgumentMapperTestCases]
	, classOf[DataFrameColumnMapperTestCases]
	, classOf[DataFrameTransformerTestCases]
	, classOf[InputArgumentCheckerTestCases]
	, classOf[OrchestratorTestCases]
	, classOf[DataFrameFilterTestCases]
	, classOf[DataFrameValidationTestCases]
)
)
class TestSuite