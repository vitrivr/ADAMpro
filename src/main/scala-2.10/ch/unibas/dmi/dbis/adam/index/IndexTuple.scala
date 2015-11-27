package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import org.apache.spark.sql.Row

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
abstract class IndexTuple { val tid: TupleID }
case class BitStringIndexTuple(tid: TupleID, value : BitString[_]) extends IndexTuple
case class IntIndexTuple(tid: TupleID, value: Int) extends IndexTuple
case class LongIndexTuple(tid: TupleID, value : Long) extends IndexTuple

object IndexTuple {
  implicit def conv_row2bitstringtuple(row: Row): BitStringIndexTuple = BitStringIndexTuple(row.getLong(0), row.getAs[BitString[_]](1))
  implicit def conv_row2longtuple(row: Row): LongIndexTuple = LongIndexTuple(row.getLong(0), row.getLong(1))
  implicit def conv_row2inttuple(row: Row): IntIndexTuple = IntIndexTuple(row.getLong(0), row.getInt(1))
}
