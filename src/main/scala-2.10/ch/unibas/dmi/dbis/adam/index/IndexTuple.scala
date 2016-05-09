package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import org.apache.spark.sql.Row

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
abstract class IndexTuple { val id: TupleID }
case class BitStringIndexTuple(id: TupleID, value : BitString[_]) extends IndexTuple
case class IntIndexTuple(id: TupleID, value: Int) extends IndexTuple
case class LongIndexTuple(id: TupleID, value : Long) extends IndexTuple
case class ByteArrayIndexTuple(id: TupleID, value : Seq[Byte]) extends IndexTuple

object IndexTuple {
  implicit def conv_row2bitstringtuple(row: Row): BitStringIndexTuple = BitStringIndexTuple(row.getAs[Long](0), row.getAs[BitString[_]](FieldNames.featureIndexColumnName))
  implicit def conv_row2longtuple(row: Row): LongIndexTuple = LongIndexTuple(row.getAs[Long](0), row.getAs[Long](FieldNames.featureIndexColumnName))
  implicit def conv_row2inttuple(row: Row): IntIndexTuple = IntIndexTuple(row.getAs[Long](0), row.getAs[Int](FieldNames.featureIndexColumnName))
  implicit def conv_row2bytearraytuple(row: Row): ByteArrayIndexTuple = ByteArrayIndexTuple(row.getAs[Long](0), row.getAs[Seq[Byte]](FieldNames.featureIndexColumnName))

}
