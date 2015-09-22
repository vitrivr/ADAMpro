package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._

import scala.collection.immutable.BitSet

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
   * @param table
   * @param q
   * @param distance
   * @param k
   * @return
   */
  def apply(table : Table, q: WorkingVector, distance : DistanceFunction, k : Int): Seq[Result] = {
    table.tuples
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
   * @param ids
   * @return
   */
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName, ids: BitSet): Seq[Result] = {
    apply(Table.retrieveTable(tablename), q, distance, k, ids)
  }

  /**
   *
   * @param table
   * @param q
   * @param distance
   * @param k
   * @param filter
   * @return
   */
  def apply(table : Table, q: WorkingVector, distance : DistanceFunction, k : Int, filter: BitSet): Seq[Result] = {
    val data = table.tuples
      .filter(tuple => filter.contains(tuple.tid.toInt))
      .map(tuple => {
      val f : WorkingVector = tuple.value
      Result(distance(q, f), tuple.tid)
    }).collect()

    data.sortBy(_.distance).take(k)
  }
}
