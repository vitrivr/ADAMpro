package ch.unibas.dmi.dbis.adam.table

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.datatypes.WorkingVectorWrapper
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID
import org.apache.spark.sql.Row

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */

case class Tuple(val tid: TupleID, val value: WorkingVector)

case class WrappingTuple(id : TupleID, value : WorkingVectorWrapper)


object Tuple {
  type TupleID = Long

  implicit def conv_row2tuple[T](value: Row): Tuple = {
    Tuple(value.getLong(0), value.getAs[WorkingVectorWrapper](1).value)
  }
}