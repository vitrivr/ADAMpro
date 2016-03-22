package ch.unibas.dmi.dbis.adam.evaluation.execution

import java.io.PrintWriter

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
trait Experiment {
  /**
    *
    * @return
    */
  def name : String

  /**
    *
    * @param writer
    */
  def startup(writer: PrintWriter)

  /**
    *
    * @param writer
    */
  def run(writer: PrintWriter)

  /**
    *
    * @param writer
    */
  def shutdown(writer: PrintWriter)

  /**
    *
    * @param k
    * @return
    */
  def getRandomVector(k: Int) = Seq.fill(k)(Random.nextFloat)

  /**
    *
    * @param block
    * @tparam R
    * @return
    */
  protected def time[R](block: => R): Long = {
    val t0 = System.nanoTime()
    block
    val t1 = System.nanoTime()
    (t1 - t0)
  }
}
