package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.Signature
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class ResultHandler(k: Int) {
  private val ls = new ListBuffer[ResultElement]()

  private var max = Float.MaxValue

  /**
   * 
   */
  def offerResultElement(it: Iterator[ResultElement]): Unit = {
    it.foreach { res =>
      if (res.lbound < max || ls.size < k) {
        ls.+=(res)
        ls.sortBy(x => x.ubound)
        max = ls(math.min(ls.size, k) - 1).ubound
      }
    }
  }

  /**
   * 
   */
  def iterator: Iterator[ResultElement] = {
    results.iterator
  }

  /**
   * 
   */
  def results = {
    ls.sortBy(x => x.ubound)
  }

}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
protected trait ResultElement {
  val indexTuple: IndexTuple[Signature]
  val ubound: Distance
  val lbound: Distance
}
