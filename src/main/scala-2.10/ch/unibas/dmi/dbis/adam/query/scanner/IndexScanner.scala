package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.log4j.Logger


/**
  * adamtwo
  *
  * Performs an index scan.
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexScanner {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Performs an index scan.
    *
    * @param index
    * @param query
    * @param filter pre-filter to use when scanning the index
    * @return
    */
  def apply(index: Index, query: NearestNeighbourQuery, filter: Option[Set[TupleID]]): Set[TupleID] = {
    log.debug("scan index")
    index.scan(query.q, query.options, query.k, filter, query.queryID)
  }
}