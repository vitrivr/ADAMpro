package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.evaluation.EvaluationConfig
import ch.unibas.dmi.dbis.adam.index.Index
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
class EvaluationIndexQueryPerformer {
  //make dirs
  new File(AdamConfig.evaluationPath).mkdirs()

  val path = AdamConfig.evaluationPath + "/" + ("results_singlequery" + System.currentTimeMillis() + ".txt")
  val pw = new PrintWriter(new File(path))
  val experiments = mutable.Queue[(Int, Int, String, Int)]()

  val dateFormat = new SimpleDateFormat("hh:mm:ss")

  /**
   *
   */
  def init() = {
    pw.write("id" + "," + "dbSize" + "," + "vecSize" + "," + "type" + "," + "time" + "," + "resultlist" + "\n")

    EvaluationConfig.dbSizes foreach { dbSize =>
      EvaluationConfig.vectorSizes foreach { vecSize =>
        EvaluationConfig.indexes foreach { indextype =>
          (0 until EvaluationConfig.numExperiments) foreach { experimentN =>
            experiments.enqueue((dbSize, vecSize, indextype.toString, experimentN))
          }
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
      val (dbSize, vecSize, indextype, experimentN) = experiments.dequeue()

      val indexname = "data_" + dbSize + "_" + vecSize + "_" + indextype + "_0"
      if (!Index.exists(indexname)) {
        throw new IllegalStateException("Index not found.");
      }

      val today = Calendar.getInstance().getTime()
      println(dbSize + "\t" + vecSize + "\t" + experimentN + "\t" + dateFormat.format(today))

      val query = NearestNeighbourQuery(getRandomVector(vecSize): FeatureVector, ManhattanDistance, EvaluationConfig.k, false)

      //TODO: add this back
      /*var results: DataFrame = _
      val runningTime = time {
        results = QueryHandler.indexQuery(indexname)(query, None, false)
      }

      pw.write(
        System.nanoTime() + "," +
          dbSize + "," +
          vecSize + "," +
          indexname + "," +
          runningTime + "," +
          results.map(_.getLong(0)).collect().mkString("{", ";", "}") +
          "\n")
      pw.flush()*/

      nextExperiment()

    } catch {
      case e: Exception => {
        e.printStackTrace()
        nextExperiment()
      }
    }
  }

  def getRandomVector(k: Int): String = Seq.fill(k)(Random.nextFloat).mkString("<", ",", ">")

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
