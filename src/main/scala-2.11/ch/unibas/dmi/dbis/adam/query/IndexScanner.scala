package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexScanner {
  def apply(q: WorkingVector, distance : DistanceFunction, k : Int, indexName: IndexName, options : Map[String, String]): Seq[Result] = {
    val index = Index.retrieveIndex(indexName)

    val filteredTuples = index.query(q, options)

    Table.retrieveTable(index.tablename).tuples
      .filter(tuple => filteredTuples.contains(tuple.tid))
      .map(tuple => {
      val f : WorkingVector = tuple.value
      Result(distance(q, f), tuple.tid)
    })
      .takeOrdered(k)

  }
}
