package ch.unibas.dmi.dbis.adam.index.structures.sh

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import it.unimi.dsi.fastutil.ints.IntHeapPriorityQueue
import org.apache.spark.sql.Row

import scala.collection.mutable.ListBuffer

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class SHResultHandler(k: Int) {
  @transient private var elementsLeft = k
  @transient private val queue =  new IntHeapPriorityQueue(2 * k)
  @transient protected var ls = ListBuffer[SHResultElement]()

  /**
    *
    * @param r
    */
  def offer(r: Row): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) {
        val score = r.getAs[Int](FieldNames.distanceColumnName)
        val tid = r.getAs[Long](FieldNames.idColumnName)
        elementsLeft -= 1
        enqueue(score, tid)
        return true
      } else {
        val peek = queue.firstInt()
        val score = r.getAs[Int](FieldNames.distanceColumnName)
        if (peek < score) {
          queue.dequeueInt()
          val tid = r.getAs[Long](FieldNames.idColumnName)
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
    * @param res
    * @return
    */
  def offer(res: SHResultElement): Boolean = {
    queue.synchronized {
      if (elementsLeft > 0) {
        elementsLeft -= 1
        enqueue(res)
        return true
      } else {
        val peek = queue.firstInt
        val score = res.score
        if (peek < score) {
          queue.dequeueInt()
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
    * @param score
    * @param tid
    */
  private def enqueue(score : Int, tid : TupleID): Unit ={
    enqueue(SHResultElement(score, tid))
  }

  /**
    * 
    * @param res
    */
  private def enqueue(res: SHResultElement): Unit = {
    queue.enqueue(res.score)
    ls += res
  }


  /**
    *
    * @return
    */
  def results = ls.sortBy(_.score).toSeq

}

case class SHResultElement(score : Int, tid: TupleID) {}