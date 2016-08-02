package ch.unibas.dmi.dbis.adam.index.structures.va

import it.unimi.dsi.fastutil.floats.{FloatComparators, FloatHeapPriorityQueue}
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[va] class VAResultHandler[A](k: Int) {
  @transient private var elementsLeft = k
  @transient private val queue =  new FloatHeapPriorityQueue(2 * k, FloatComparators.OPPOSITE_COMPARATOR)
  @transient protected var ls = ListBuffer[VAResultElement[A]]()

  /**
    *
    * @param r
    */
  def offer(r: Row, pk : String): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) {
        val lower = r.getAs[Float]("lbound")
        val upper = r.getAs[Float]("ubound")
        val tid = r.getAs[A](pk)
        elementsLeft -= 1
        enqueue(lower, upper, tid)
        return true
      } else {
        val peek = queue.firstFloat()
        val lower = r.getAs[Float]("lbound")
        if (peek >= lower) {
          queue.dequeueFloat()
          val upper = r.getAs[Float]("ubound")
          val tid = r.getAs[A](pk : String)
          enqueue(lower, upper, tid)
          return true
        } else {
          return false
        }
      }
    }
  }

  /**
    *
    * @param lower
    * @param upper
    * @param tid
    */
  private def enqueue(lower : Float, upper : Float, tid : A): Unit ={
    enqueue(VAResultElement(lower, upper, tid))
  }

  /**
    *
    * @param res
    */
  private def enqueue(res: VAResultElement[A]): Unit = {
    queue.enqueue(res.upper)
    ls += res
  }


  /**
    *
    * @return
    */
  def results = ls.toSeq

}

case class VAResultElement[A](lower: Float, upper : Float, tid: A) {}