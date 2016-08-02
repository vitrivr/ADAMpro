package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.config.FieldNames
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
      if (elementsLeft > 0) {
        val score = r.getAs[Int](FieldNames.distanceColumnName)
        val tid = r.getAs[A](pk)
        elementsLeft -= 1
        enqueue(score, tid)
        return true
      } else {
        val peek = queue.firstInt
        val score = r.getAs[Int](FieldNames.distanceColumnName)
        if (peek >= score) {
          queue.dequeueInt()
          val tid = r.getAs[A](pk)
          enqueue(score, tid)
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
  private def enqueue(score : Int, tid : A): Unit ={
    enqueue(SHResultElement(score, tid))
  }

  /**
    * 
    * @param res
    */
  private def enqueue(res: SHResultElement[A]): Unit = {
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