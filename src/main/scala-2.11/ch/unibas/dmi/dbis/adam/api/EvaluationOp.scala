package ch.unibas.dmi.dbis.adam.api

import java.io.{File, PrintWriter}

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.query.{ProgressiveQueryStatus, QueryHandler, Result}
import ch.unibas.dmi.dbis.adam.table.Table

import scala.collection.mutable
import scala.util.Random


/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object EvaluationOp {
  val dbSizes = Seq(1000, 1000,10000,100000,1000000,10000000)
  val vectorSizes = Seq(10, 50, 100, 200, 500)
  val k = 100
  val numExperiments = 10

  val pw = new PrintWriter(new File("results_" + System.currentTimeMillis() +".txt"))
  val experiments = mutable.Queue[(Int, Int)]()

  def apply() = {
    pw.write("id" + "," + "dbSize" + "," + "vecSize" + "," + "type" + "," + "measure1" + "," + "measure2" + "," + "resultlist" + "\n")

    dbSizes foreach { dbSize =>
      vectorSizes foreach { vecSize =>
        (0 until numExperiments) foreach { exp =>
          experiments.enqueue((dbSize, vecSize))
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
      if(experiments.isEmpty){
      return
    })


    val (dbSize, vecSize) = experiments.dequeue()

    val tabname = "data_" + dbSize + "_" + vecSize

    if(Table.existsTable(tabname)){
      try {
        QueryHandler.progressiveQuery(getRandomVector(vecSize) : WorkingVector,
          NormBasedDistanceFunction(1), k, tabname,
          onComplete(System.nanoTime(), dbSize, vecSize))
      } catch {
        case e : Exception =>
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
  def onComplete(startTime : Long, dbSize : Int, vecSize : Int)(status : ProgressiveQueryStatus, results : Seq[Result], options : Map[String, String]) {
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

    if(status.allEnded){
      println("Completed: data_" + dbSize + "_" + vecSize)
      Thread.sleep(5000L)
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
