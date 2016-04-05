package ch.unibas.dmi.dbis.adam.index.structures.lsh.results

import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.BitStringIndexTuple
import ch.unibas.dmi.dbis.adam.index.utils.FixedSizePriorityQueue

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * September 2015
  */
class LSHResultHandler(k: Int) extends Serializable {
  @transient var ls = ListBuffer[ResultElement]()
  @transient var queue = new FixedSizePriorityQueue[Int](k)

  def iterator: Iterator[ResultElement] = results.iterator

  def results = ls.sortBy(_.score)

  /**
    *
    * @param tuple
    */
  def offer(tuple: BitStringIndexTuple, score: Int): Unit = {
    if (queue.offer(score)) {
      ls += ResultElement(score, tuple.id)
    }
  }


  /**
    *
    * @param res
    */
  def offer(res: ResultElement): Unit = {
    if (queue.offer(res.score)) {
      ls += res
    }
  }
}

case class ResultElement(val score: Int, val tid: TupleID) {}