package com.arjun.validation

import org.slf4j.LoggerFactory

class InputArgumentChecker {

	val logger = LoggerFactory.getLogger(classOf[InputArgumentChecker])

	def checkArguments(arguments: Array[String]): Boolean = {

		logger.info("Inside checkArguments ")

		if ( arguments == null ) {
			false
		} else if ( arguments.length == 0 ) false else true

	}
}
