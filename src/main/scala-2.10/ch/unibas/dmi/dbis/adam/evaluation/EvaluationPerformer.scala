package ch.unibas.dmi.dbis.adam.evaluation

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.main.Startup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.ManhattanDistance
import ch.unibas.dmi.dbis.adam.query.handler.NearestNeighbourQueryHandler
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery

import scala.collection.mutable
import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
class EvaluationPerformer {
  val path = Startup.config.evaluationPath / ("results_" + System.currentTimeMillis() + ".txt")
  val pw = new PrintWriter(new File(path.toAbsolute.toString()))
  val experiments = mutable.Queue[(Int, Int, Int)]()

  val dateFormat = new SimpleDateFormat("hh:mm:ss")

  /**
   *
   */
  def start() = {
    pw.write("id" + "," + "dbSize" + "," + "vecSize" + "," + "type" + "," + "measure1" + "," + "measure2" + "," + "resultlist" + "\n")

    EvaluationConfig.dbSizes foreach { dbSize =>
      EvaluationConfig.vectorSizes foreach { vecSize =>
        (0 until EvaluationConfig.numExperiments) foreach { experimentN =>
          experiments.enqueue((dbSize, vecSize, experimentN))
        }
      }
    }

    nextExperiment()
  }

  /**
   *
   * @return
   */
  def nextExperiment() {
    experiments.synchronized(
      if (experiments.isEmpty) {
        return
      })

    try {
      val (dbSize, vecSize, experimentN) = experiments.dequeue()

      val entityname = "data_" + dbSize + "_" + vecSize
      if (!Entity.existsEntity(entityname)) {
        throw new IllegalStateException("Exception thrown");
      }

      val today = Calendar.getInstance().getTime()
      println(dbSize + "\t" + vecSize + "\t" + experimentN + "\t" + dateFormat.format(today))

      val query = NearestNeighbourQuery(getRandomVector(vecSize): FeatureVector, ManhattanDistance, EvaluationConfig.k, false)
      NearestNeighbourQueryHandler.progressiveQuery(entityname, query, None, onComplete(System.nanoTime(), dbSize, vecSize))

    } catch {
      case e: Exception => {
        println(e.getMessage)
      }
    } finally {
      nextExperiment()
    }
  }

  /**
   *
   * @param startTime
   * @param vecSize
   * @param dbSize
   * @param results
   * @param options
   * @return
   */
  def onComplete(startTime: Long, dbSize: Int, vecSize: Int)(status: ProgressiveQueryStatus.Value, results: Seq[Result], confidence: Float, options: Map[String, String]) {
    pw.write(
      options.getOrElse("qid", "") + "," +
        vecSize + "," +
        dbSize + "," +
        options.getOrElse("type", "") + "," +
        System.nanoTime() + "," +
        startTime + "," +
        results.map(_.tid).mkString("{", ";", "}") +
        "\n")
    pw.flush()

    if (status == ProgressiveQueryStatus.FINISHED) {
      Thread.sleep(1000L)
      nextExperiment()
    }
  }

  /**
   *
   * @param k
   * @return
   */
  def getRandomVector(k: Int): String = {
    Seq.fill(k)(Random.nextFloat).mkString("<", ",", ">")
  }

  /**
   *
   * @param block
   * @tparam R
   * @return
   */
  def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    block
    val t1 = System.nanoTime()
    (t1 - t0)
  }
}
