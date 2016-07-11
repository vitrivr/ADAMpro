package ch.unibas.dmi.dbis.adam.chronos

import java.io.File
import java.util.Properties

import ch.unibas.cs.dbis.chronos.agent.{AbstractChronosAgent, ChronosJob}

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class ChronosAgent(ipAddressOrHostname: String) extends AbstractChronosAgent(ipAddressOrHostname, 80, false, true) {
  val runningJobs = mutable.Map[Int, EvaluationExecutor]()

  /**
    *
    * @param job
    */
  override def aborded(job: ChronosJob): Unit = {
    val executor = runningJobs.get(job.id)

    if(executor.isDefined){
      executor.get.abort()
    }
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
    val logger = addChronosLogHandler(job)
    val executor = new EvaluationExecutor(job, logger, setProgress(job)(_), inputDirectory, outputDirectory)

    val thread = new Thread {
      override def run {
        // your custom behavior here
      }
    }
    thread.start
    Thread.sleep(50) // slow the loop down a bit



    runningJobs +=  job.id -> executor
    val results = executor.run()
    runningJobs -= job.id

    results
  }

  /**
    *
    * @param job
    * @param status
    */
  private def setProgress(job : ChronosJob)(status : Double) : Boolean = {
    this.setExecutionStatus(job.id, math.floor(status * 100).toByte)
  }
}

object ChronosAgent {
  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    new ChronosAgent("chronos.dmi.unibas.ch").run()
  }
}
