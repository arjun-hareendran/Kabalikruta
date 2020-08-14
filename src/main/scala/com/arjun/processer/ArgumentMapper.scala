package com.arjun.processer

import com.arjun.datastructures.ArgumentHolder
import org.slf4j.LoggerFactory

class ArgumentMapper {

  val logger = LoggerFactory.getLogger(classOf[ArgumentMapper])

  def mapCommandLineArguments(inputArguments:Array[String]): ArgumentHolder ={

    logger.info("Inside mapCommandLineArguments")
      new ArgumentHolder(inputArguments(0),inputArguments(1),inputArguments(2))
  }

}
