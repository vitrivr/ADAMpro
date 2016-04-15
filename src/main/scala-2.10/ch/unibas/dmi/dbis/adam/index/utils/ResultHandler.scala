package ch.unibas.dmi.dbis.adam.index.utils

import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.IndexTuple

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * September 2015
  */
class ResultHandler[A](k: Int)(implicit val ordering: Ordering[A]) extends Serializable {
  @transient protected var ls = ListBuffer[ResultElement[A]]()
  @transient protected var queue = new FixedSizePriorityQueue[A](k)(ordering)

  def iterator: Iterator[ResultElement[A]] = results.iterator

  def results = ls.sortBy(_.score)

  /**
    *
    * @param tuple
    */
  def offer(tuple: IndexTuple, score: A): Unit = offer(new ResultElement(score, tuple.id))


  /**
    *
    * @param res
    */
  def offer(res: ResultElement[A]): Unit = {
    if (queue.offer(res.compareScore, res.insertScore)) {
      ls += res
    }
  }
}

class ResultElement[A](val score: A, val tid: TupleID) {
  def insertScore = score
  def compareScore = score
}