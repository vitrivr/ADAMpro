package org.vitrivr.adampro.index.structures.sh

import org.vitrivr.adampro.config.FieldNames
import it.unimi.dsi.fastutil.ints.{IntComparators, IntHeapPriorityQueue}
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class SHResultHandler[A](k: Int) {
  @transient private var elementsLeft = k
  @transient private val queue =  new IntHeapPriorityQueue(2 * k, IntComparators.OPPOSITE_COMPARATOR)
  @transient protected var ls = ListBuffer[SHResultElement[A]]()

  /**
    *
    * @param r
    */
  def offer(r: Row, pk : String): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) { //we have not yet inserted k elements, no checks therefore
        val score = r.getAs[Int](FieldNames.distanceColumnName)
        val tid = r.getAs[A](pk)
        elementsLeft -= 1
        enqueueAndAddToCandidates(score, tid)
        return true
      } else { //we have already k elements, therefore check if new element is better
        val peek = queue.firstInt
        val score = r.getAs[Int](FieldNames.distanceColumnName)
        if (peek >= score) {
          //if peek is larger than lower, then dequeue worst element and insert
          //new element
          queue.dequeueInt()
          val tid = r.getAs[A](pk)
          enqueueAndAddToCandidates(score, tid)
          return true
        } else {
          return false
        }
      }
    }
  }

  /**
    *
    * @param score
    * @param tid
    */
  private def enqueueAndAddToCandidates(score : Int, tid : A): Unit ={
    enqueueAndAddToCandidates(SHResultElement(score, tid))
  }

  /**
    * 
    * @param res
    */
  private def enqueueAndAddToCandidates(res: SHResultElement[A]): Unit = {
    queue.enqueue(res.score)
    ls += res
  }


  /**
    *
    * @return
    */
  def results = ls.sortBy(_.score).toSeq
}

case class SHResultElement[A](score : Int, tid: A) {}