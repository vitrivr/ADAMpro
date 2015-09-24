package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.results

import ch.unibas.dmi.dbis.adam.index.IndexTuple
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
  @transient var queue = MinMaxPriorityQueue.orderedBy(scala.math.Ordering.Int).maximumSize(k).create[Int]
  @transient var max = Float.MaxValue


  /**
   *
   * @param tuple
   */
  def offerIndexTuple(tuple: IndexTuple, score : Int): Unit = {
    if(score < max || queue.size < k){
      ls += ResultElement(score, tuple.tid)
      queue.add(score)
      max = queue.peekLast()
    }
  }


  /**
   *
   * @param it
   */
  def offerResultElement(it: Iterator[ResultElement]): Unit = {
    while (it.hasNext) {
      val res = it.next()
      if(res.score < max || queue.size < k){
        ls += res
        queue.add(res.score)
        max = queue.peekLast()
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


