package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.evaluation.EvaluationConfig
import ch.unibas.dmi.dbis.adam.query.distance.ManhattanDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery

import scala.collection.mutable
import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
class EvaluationSequentialQueryPerformer {
  //make dirs
  new File(AdamConfig.evaluationPath).mkdirs()

  val path = AdamConfig.evaluationPath + "/" + ("results_singlequery" + System.currentTimeMillis() + ".txt")
  val pw = new PrintWriter(new File(path))
  val experiments = mutable.Queue[(Int, Int, Int)]()

  val dateFormat = new SimpleDateFormat("hh:mm:ss")

  /**
   *
   */
  def init() = {
    pw.write("id" + "," + "dbSize" + "," + "vecSize" + "," + "time" + "," + "resultlist" + "\n")

    EvaluationConfig.dbSizes foreach { dbSize =>
      EvaluationConfig.vectorSizes foreach { vecSize =>
        (0 until EvaluationConfig.numExperiments) foreach { experimentN =>
          experiments.enqueue((dbSize, vecSize, experimentN))
        }
      }
    }
  }

  /**
   *
   */
  def start() = nextExperiment()

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
      if (!Entity.exists(entityname)) {
        throw new IllegalStateException("Entity not found.");
      }

      val today = Calendar.getInstance().getTime()
      println(dbSize + "\t" + vecSize + "\t" + experimentN + "\t" + dateFormat.format(today))

      val query = NearestNeighbourQuery(getRandomVector(vecSize): FeatureVector, ManhattanDistance, EvaluationConfig.k, false)

      //TODO: add this back
      /*var results: DataFrame = _
      val runningTime = time {
        results = QueryHandler.sequentialQuery(entityname)(query, None, false)
      }

      pw.write(
        System.nanoTime() + "," +
          dbSize + "," +
          vecSize + "," +
          runningTime + "," +
          results.map(_.getLong(0)).collect().mkString("{", ";", "}") +
          "\n")
      pw.flush()*/

      nextExperiment()

    } catch {
      case e: Exception => {
        println("ERROR: " + e.getMessage)
        nextExperiment()
      }
    }
  }

  /**
   *
   * @param dims
   * @return
   */
  private def getRandomVector(dims: Int): String = Seq.fill(dims)(Random.nextFloat).mkString("<", ",", ">")

  /**
   *
   * @param block
   * @tparam R
   * @return
   */
  private def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    block
    val t1 = System.nanoTime()
    (t1 - t0)
  }
}
