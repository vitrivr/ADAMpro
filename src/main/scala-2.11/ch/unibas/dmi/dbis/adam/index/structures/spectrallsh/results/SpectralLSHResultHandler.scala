package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.results

import ch.unibas.dmi.dbis.adam.index.BitStringIndexTuple
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID
import com.google.common.collect.MinMaxPriorityQueue

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class SpectralLSHResultHandler(k: Int) extends Serializable {
  @transient var ls = ListBuffer[ResultElement]()
  @transient var queue = MinMaxPriorityQueue.orderedBy(scala.math.Ordering.Int.reverse).maximumSize(k).create[Int]
  @transient var min = Float.MinValue


  /**
   *
   * @param tuple
   */
  def offerIndexTuple(tuple: BitStringIndexTuple, score : Int): Unit = {
    if(score >= min || queue.size < k){
      ls += ResultElement(score, tuple.tid)
      queue.add(score)
      min = queue.peekLast()
    }
  }


  /**
   *
   * @param it
   */
  def offerResultElement(it: Iterator[ResultElement]): Unit = {
    while (it.hasNext) {
      val res = it.next()
      if(res.score >= min || queue.size < k){
        ls += res
        queue.add(res.score)
        min = queue.peekLast()
      }
    }
  }

  /**
   *
   * @return
   */
  def iterator: Iterator[ResultElement] = {
    results.iterator
  }

  /**
   *
   * @return
   */
  def results = {
    ls.sortBy(_.score)
  }
}


case class ResultElement(val score : Int, val tid : TupleID) {}


