package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.table.Tuple
import Tuple.TupleID

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
//@SQLUserDefinedType(udt = classOf[IndexTupleUDT])
case class IndexTuple(tid: TupleID, bits: BitString[_])

/*class IndexTupleUDT extends UserDefinedType[IndexTuple] {
  /**
   *
   * @return
   */
  override def sqlType: DataType =
    StructType(Seq(StructField("id", LongType), StructField("value", BinaryType)))

  /**
   *
   * @param obj
   * @return
   */
  override def serialize(obj: Any): Row = {
    obj match {
      case dt: IndexTuple =>
        val row = new GenericMutableRow(2)
        row.setLong(0, dt.tid)
        row.update(1, dt.bits.toByteSeq)
        row
    }
  }

  /**
   *
   * @param datum
   * @return
   */
  override def deserialize(datum: Any): IndexTuple = {
    datum match {
      case row: Row =>
        require(row.length == 2)
        val tid = row.getLong(0)
        val value = new MinimalBitString(util.BitSet.valueOf(row.getSeq[Byte](1).toArray))
        IndexTuple(tid, value)
    }
  }

  /**
   *
   * @return
   */
  override def userClass: Class[IndexTuple] = classOf[IndexTuple]

  /**
   *
   * @return
   */
  override def asNullable: IndexTupleUDT = this
}*/