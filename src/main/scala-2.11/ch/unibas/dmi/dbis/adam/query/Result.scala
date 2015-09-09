package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.table.Tuple
import Tuple.TupleID
import ch.unibas.dmi.dbis.adam.query.distance.Distance.Distance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class Result (distance : Distance, tid : TupleID)  extends Ordered[Result] {
  /**
   *
   * @param that
   * @return
   */
  override def compare(that: Result): Int = distance compare that.distance
}
