package ch.unibas.dmi.dbis.adam.chronos

import java.io.File
import java.util.Properties

import ch.unibas.cs.dbis.chronos.agent.{AbstractChronosAgent, ChronosJob}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class ChronosAgent(ipAddressOrHostname: String) extends AbstractChronosAgent(ipAddressOrHostname, 80, false, false) {

  def main(args: Array[String]): Unit = {
    val agent = new ChronosAgent("10.34.58.141")
    agent.run()
  }

  /**
    *
    * @param job
    */
  override def aborded(job: ChronosJob): Unit = {
    //TODO: handle aborted status
  }

  /**
    *
    * @return
    */
  override def getSupportedSystemNames: Array[String] = Array("adampro")

  /**
    *
    * @param job
    * @param inputDirectory
    * @param outputDirectory
    * @return
    */
  override def execute(job: ChronosJob, inputDirectory : File, outputDirectory : File): Properties = {
    //TODO
    return new Properties()
  }
}
