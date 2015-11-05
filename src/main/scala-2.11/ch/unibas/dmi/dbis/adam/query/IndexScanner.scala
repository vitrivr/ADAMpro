package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.{IndexTuple, Index}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery

import scala.collection.immutable.HashSet


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexScanner {
  def apply(index : Index[_ <: IndexTuple], query : NearestNeighbourQuery, filter : Option[HashSet[TupleID]]): HashSet[TupleID] = {
    index.scan(query.q, query.options, query.k, filter, query.queryID)
  }
}