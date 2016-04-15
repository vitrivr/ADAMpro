package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.index.utils.ResultElement
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
  @transient protected var ls = ListBuffer[ResultElement[Float]]()

  /**
    *
    * @param r
    */
  def offer(r: Row): Unit = {
    var result = false

    queue.synchronized {
      if (elementsLeft > 0) {
        val upper = r.getAs[Float]("ubound")
        queue.enqueue(upper)
        elementsLeft -= 1
        result = true
      } else {
        val peek = queue.firstFloat()
        val lower = r.getAs[Float]("lbound")
        if (peek > lower) {
          queue.dequeueFloat()
          queue.enqueue(r.getAs[Float]("ubound"))
          result = true
        } else {
          result = false
        }
      }
    }

    if (result) {
      ls += new ResultElement(0.toFloat, r.getAs[Long](FieldNames.idColumnName))
    }

  }

  def results = ls.toSeq
}