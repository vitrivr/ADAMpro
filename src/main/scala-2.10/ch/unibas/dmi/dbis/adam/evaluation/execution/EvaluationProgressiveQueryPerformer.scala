package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.evaluation.EvaluationConfig
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.ManhattanDistance
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.sql.DataFrame

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
//TODO: clean up with other performer
class EvaluationProgressiveQueryPerformer {
  //make dirs
  new File(AdamConfig.evaluationPath).mkdirs()

  val path = AdamConfig.evaluationPath + "/" + ("results_progressivequery" + System.currentTimeMillis() + ".txt")
  val pw = new PrintWriter(new File(path))
  val experiments = mutable.Queue[(Int, Int, Int)]()

  val dateFormat = new SimpleDateFormat("hh:mm:ss")

  /**
   *
   */
  def init() = {
    pw.write("id" + "," + "dbSize" + "," + "vecSize" + "," + "type" + "," + "time1" + "," + "time2" + "," + "resultlist" + "\n")

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

      import scala.concurrent.ExecutionContext.Implicits.global
      val tracker = QueryHandler.progressiveQuery(entityname)(query, None, onComplete(System.nanoTime(), dbSize, vecSize), false)
      Await.ready(Future {
        while (tracker.status == ProgressiveQueryStatus.RUNNING) {
          Thread.sleep(10000L)
        }
      }, Duration.apply(2000, "seconds"))
      println("done")
      tracker.stop()

      Await.ready(Future {
          Thread.sleep(10000L)
      }, Duration.apply(15, "seconds"))

      SparkStartup.sc.cancelAllJobs()

      Await.ready(Future {
        Thread.sleep(10000L)
      }, Duration.apply(15, "seconds"))

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
   * @param startTime
   * @param vecSize
   * @param dbSize
   * @param results
   * @param options
   * @return
   */
  def onComplete(startTime: Long, dbSize: Int, vecSize: Int)(status: ProgressiveQueryStatus.Value, results: DataFrame, confidence: Float, options: Map[String, String]) {
    pw.write(
      options.getOrElse("qid", "") + "," +
        dbSize + "," +
        vecSize + "," +
        options.getOrElse("type", "") + "," +
        System.nanoTime() + "," +
        startTime + "," +
        results.map(_.getLong(0)).collect().mkString("{", ";", "}") +
        "\n")
    pw.flush()
  }

  /**
   *
   * @param dims
   * @return
   */
  private def getRandomVector(dims: Int): String = Seq.fill(dims)(Random.nextFloat).mkString("<", ",", ">")
}
