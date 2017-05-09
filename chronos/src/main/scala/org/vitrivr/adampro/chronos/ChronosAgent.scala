package org.vitrivr.adampro.chronos

import java.io.File
import java.util.Properties
import java.util.logging.Logger

import ch.unibas.dmi.dbis.chronos.agent.{AbstractChronosAgent, ChronosJob}
import org.vitrivr.adampro.chronos.executors._

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class ChronosAgent(ipAddressOrHostname: String, environment: String) extends AbstractChronosAgent(ipAddressOrHostname, 80, false, true, environment) {
  val runningJobs = mutable.Map[Int, Executor]()

  /**
    *
    * @param job
    */
  override def aborted(job: ChronosJob): Unit = {
    val executor = runningJobs.get(job.id)

    if (executor.isDefined) {
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
  override def execute(job: ChronosJob, inputDirectory: File, outputDirectory: File): Properties = {
    val ejob = new EvaluationJob(job)

    val executor = ejob.general_mode match {
      case "eqe" => new EQEExecutor(ejob, setProgress(job)(_), inputDirectory, outputDirectory)
      case "pae" => new PAEExecutor(ejob, setProgress(job)(_), inputDirectory, outputDirectory)
      case "pqe" => new PQEExecutor(ejob, setProgress(job)(_), inputDirectory, outputDirectory)
      case "see" => new SEEExecutor(ejob, setProgress(job)(_), inputDirectory, outputDirectory)
      case "sen" => new SENExecutor(ejob, setProgress(job)(_), inputDirectory, outputDirectory)
      case "sqe" => new SQEExecutor(ejob, setProgress(job)(_), inputDirectory, outputDirectory)
    }

    runningJobs += job.id -> executor
    val results = executor.run()
    runningJobs -= job.id
    results
  }

  /**
    *
    * @param job
    * @param status
    */
  private def setProgress(job: ChronosJob)(status: Double): Boolean = {
    this.setProgress(job, math.floor(status * 100).toByte)
  }
}

object ChronosAgent {
  private val LOG = Logger.getLogger(classOf[AbstractChronosAgent].getName)

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val environment = if (args.length > 0) {
      LOG.info("starting agent with environment '" + args(0) + "'")

      args(0)
    } else {
      null
    }

    new ChronosAgent("chronos.dmi.unibas.ch", environment).run()
  }
}
