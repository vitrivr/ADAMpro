package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
abstract class IndexTuple{
  val tid: TupleID
}

case class BitStringIndexTuple(tid: TupleID, value : BitString[_]) extends IndexTuple

case class IntIndexTuple(tid: TupleID, value: Int) extends IndexTuple

case class LongIndexTuple(tid: TupleID, value : Long) extends IndexTuple
