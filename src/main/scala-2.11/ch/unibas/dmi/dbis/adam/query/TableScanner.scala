package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object TableScanner {
  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   * @return
   */
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName): Seq[Result] = {
    Table.retrieveTable(tablename).tuples
      .map(tuple => {
      val f : WorkingVector = tuple.value
      Result(distance(q, f), tuple.tid)
    })
      .sortBy(_.distance).take(k)
  }

  /**
   *
   * @param q
   * @param distance
   * @param k
   * @param tablename
   * @param filter
   * @return
   */
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName, filter: Seq[TupleID]): Seq[Result] = {
    val data = Table.retrieveTable(tablename).tuples
      .filter(tuple => filter.contains(tuple.tid))
      .map(tuple => {
      val f : WorkingVector = tuple.value
      Result(distance(q, f), tuple.tid)
    }).collect()

      data.sortBy(_.distance).take(k)
  }
}
