package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.data.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class IndexTuple(tid: TupleID, value: BitString[_])