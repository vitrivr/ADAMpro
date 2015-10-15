package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class IndexTuple(tid: TupleID)

case class BitStringIndexTuple(tid: TupleID, bits: BitString[_])

case class IntIndexTuple(tid: TupleID, bits: Int)

case class LongIndexTuple(tid: TupleID, bits: Long)
