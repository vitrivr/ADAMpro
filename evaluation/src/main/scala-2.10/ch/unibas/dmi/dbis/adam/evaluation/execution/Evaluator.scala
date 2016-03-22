package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.{File, PrintWriter}

import ch.unibas.dmi.dbis.adam.evaluation.config.EvaluationConfig
import org.apache.logging.log4j.LogManager

import scala.collection.mutable

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object Evaluator {
  val log = LogManager.getLogger

  new File(EvaluationConfig.evaluationPath).mkdirs()

  private val experiments = mutable.Queue[Experiment]()

  /**
    *
    * @param experiment
    */
  def enqueue(experiment: Experiment): Unit = {
    experiments.enqueue(experiment)
  }

  /**
    *
    */
  def start() = {
    nextExperiment()
  }

  /**
    *
    */
  def nextExperiment() {

    experiments.synchronized(
      if (experiments.isEmpty) {
        return
      })

    var experiment: Experiment = null
    var path: File = null
    var writer: PrintWriter = null

    try {
      experiment = experiments.dequeue()
      path = new File(EvaluationConfig.evaluationPath + "/" + (experiment.name + System.currentTimeMillis() + ".csv"))
      writer = new PrintWriter(path)

      experiment.startup(writer)
    } catch {
      case e: Exception => {
        log.error("evaluator raised exception in pre-evaluation phase")
        e.printStackTrace()
      }
    }

    try {
      experiment.run(writer)
    } catch {
      case e: Exception => {
        log.error("evaluator raised exception in evaluation phase")
        e.printStackTrace()
      }
    }

    try {
      experiment.shutdown(writer)
    } catch {
      case e: Exception => {
        log.error("evaluator raised exception in post-evaluation phase")
        e.printStackTrace()
      }
    }


    writer.close()
  }


}