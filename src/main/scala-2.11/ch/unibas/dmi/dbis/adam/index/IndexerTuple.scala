package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.table.Tuple
import Tuple._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
case class IndexerTuple[A](tid: TupleID, value: A)