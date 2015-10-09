package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

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
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "table")

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
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "table")

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
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, tablename: TableName, ids: HashSet[Long]): Seq[Result] = {
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
  def apply(table : Table, q: WorkingVector, distance : DistanceFunction, k : Int, filter: HashSet[Long]): Seq[Result] = {
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "index")

    val data = table.rowsForKeys(filter).collect()

    val it = data.par.iterator

    val ls = ListBuffer[Result]()
    while(it.hasNext){
      val tuple = it.next
      val f : WorkingVector = tuple.value
      ls += Result(distance(q, f), tuple.tid)
    }

    ls.sortBy(_.distance).take(k)
  }
}
