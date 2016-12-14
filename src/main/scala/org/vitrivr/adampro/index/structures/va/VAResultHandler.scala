package org.vitrivr.adampro.index.structures.va

import java.util.Comparator

import it.unimi.dsi.fastutil.floats.{FloatComparator, FloatComparators, FloatHeapPriorityQueue}
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue
import org.apache.spark.sql.Row
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco, improvements by Silvan Heller
  * August 2015
  */
private[va] class VAResultHandler[A](k: Int) extends Logging with Serializable {
  private var elementsLeft = k

  private val upperBoundQueue = new FloatHeapPriorityQueue(2 * k, FloatComparators.OPPOSITE_COMPARATOR)
  private val lowerBoundResultElementQueue = new ObjectHeapPriorityQueue[VAResultElement[A]](2 * k, new VAResultElementLowerBoundComparator(FloatComparators.OPPOSITE_COMPARATOR))

  private class VAResultElementLowerBoundComparator(comparator: FloatComparator) extends Comparator[VAResultElement[_]] with Serializable {
    final def compare(a: VAResultElement[_], b: VAResultElement[_]): Int = comparator.compare(a.lower, b.lower)
  }


  /**
    *
    * @param r
    */
  def offer(r: Row, pk: String): Boolean = {
    if (elementsLeft > 0) {
      //we have not yet inserted k elements, no checks therefore
      val lower = r.getAs[Float]("ap_lbound")
      val upper = r.getAs[Float]("ap_ubound")
      val tid = r.getAs[A](pk)
      elementsLeft -= 1
      enqueueAndAddToCandidates(lower, upper, tid)
      return true
    } else {
      this.synchronized {
        //we have already k elements, therefore check if new element is better
        //peek is the upper bound
        val peek = upperBoundQueue.firstFloat()
        val lower = r.getAs[Float]("ap_lbound")
        if (peek >= lower) {
          //if peek is larger than lower, then dequeue worst element and insert new element
          upperBoundQueue.dequeueFloat()
          val upper = r.getAs[Float]("ap_ubound")
          val tid = r.getAs[A](pk: String)
          enqueueAndAddToCandidates(lower, upper, tid)
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
  private def enqueueAndAddToCandidates(lower: Float, upper: Float, tid: A): Unit = {
    enqueueAndAddToCandidates(VAResultElement(lower, upper, tid))
  }

  /**
    *
    * @param res
    */
  private def enqueueAndAddToCandidates(res: VAResultElement[A]): Unit = {
    this.synchronized {
      upperBoundQueue.enqueue(res.upper)
      lowerBoundResultElementQueue.enqueue(res)
      while (!lowerBoundResultElementQueue.isEmpty && lowerBoundResultElementQueue.first().lower > upperBoundQueue.firstFloat()) {
        lowerBoundResultElementQueue.dequeue()
      }
    }
  }


  /**
    *
    * @return
    */
  def results = {
    val ls = ListBuffer[VAResultElement[A]]()

    this.synchronized {
      while (lowerBoundResultElementQueue.size() > 0) {
        ls += lowerBoundResultElementQueue.dequeue()
      }
    }

    ls.toSeq
  }

}

case class VAResultElement[A](lower: Float, upper: Float, tid: A) extends Serializable {}

