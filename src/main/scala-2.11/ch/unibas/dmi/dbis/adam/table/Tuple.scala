package ch.unibas.dmi.dbis.adam.table

import ch.unibas.dmi.dbis.adam.datatypes.Feature
import Feature.StoredVector
import org.apache.spark.sql.Row

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class Tuple(tid : TupleID, value : StoredVector)

object Tuple {
  type TupleID = Long

  implicit def conv_row2tuple[T](value : Row) : Tuple = {
    Tuple(value.getInt(0), value.getSeq[Float](1))
  }
}