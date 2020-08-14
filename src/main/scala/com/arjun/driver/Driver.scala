package com.arjun.driver

import com.arjun.processer.ArgumentMapper

object Driver {

  def main(args: Array[String]): Unit = {

    val orchestrator = new Orchestrator

    val agrumentHolder = new ArgumentMapper().mapCommandLineArguments(args)
    orchestrator.argumentHolder = agrumentHolder

    orchestrator.setUp(args)
    orchestrator.process(args)

  }

}
