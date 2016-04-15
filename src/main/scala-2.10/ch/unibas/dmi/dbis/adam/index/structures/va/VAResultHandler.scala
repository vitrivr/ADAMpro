package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import it.unimi.dsi.fastutil.floats.{FloatComparators, FloatHeapPriorityQueue}
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[va] class VAResultHandler(k: Int) {
  val queue =  new FloatHeapPriorityQueue(FloatComparators.OPPOSITE_COMPARATOR)
  var elementsLeft = k
  @transient protected var ls = ListBuffer[VAResultElement]()

  /**
    *
    * @param r
    */
  def offer(r: Row): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) {
        val lower = r.getAs[Float]("lbound")
        val upper = r.getAs[Float]("ubound")
        val tid = r.getAs[Long](FieldNames.idColumnName)
        elementsLeft -= 1
        enqueue(lower, upper, tid)
        return true
      } else {
        val peek = queue.firstFloat()
        val lower = r.getAs[Float]("lbound")
        if (peek > lower) {
          queue.dequeueFloat()
          val upper = r.getAs[Float]("ubound")
          val tid = r.getAs[Long](FieldNames.idColumnName)
          enqueue(lower, upper, tid)
          return true
        } else {
          return false
        }
      }
    }
  }


  def offer(res: VAResultElement): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) {
        elementsLeft -= 1
        enqueue(res)
        return true
      } else {
        val peek = queue.firstFloat()
        val lower = res.lower
        if (peek > lower) {
          queue.dequeueFloat()
          enqueue(res)
          return true
        } else {
          return false
        }
      }
    }
  }


  /**
    *
    */
  private def enqueue(lower : Float, upper : Float, tid : TupleID): Unit ={
    enqueue(VAResultElement(lower, upper, tid))
  }

  /**
    *
    * @param res
    */
  private def enqueue(res: VAResultElement): Unit = {
    queue.enqueue(res.upper)
    ls += res
  }


  /**
    *
    * @return
    */
  def results = ls.toSeq

}

case class VAResultElement(lower: Float, upper : Float, tid: TupleID) {}