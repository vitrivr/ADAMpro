package org.vitrivr.adampro.data.datatypes.vector

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.vitrivr.adampro.data.datatypes.Wrapper

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * February 2018
  */
case class Bit64VectorWrapper(vector : Long) {
  /**
    *
    * @return
    */
  def toRow() = Bit64VectorWrapper.toRow(vector)
}

object Bit64VectorWrapper extends Wrapper {
  val datatype: DataType = StructType(Seq(
    StructField("vector", LongType)
  ))

  /**
    *
    * @param vector
    * @return
    */
  def toRow(vector : Long) = {
    Row(vector)
  }

  /**
    *
    * @return
    */
  def emptyRow = toRow(0)


  /**
    *
    * @param r
    */
  def fromRow(r : Row): Bit64VectorWrapper = Bit64VectorWrapper(r.getLong(0))


  /**
    *
    * @param d
    * @return
    */
  def fitsType(d: DataType) = {
    if (d.isInstanceOf[StructType] &&
      d.asInstanceOf[StructType].fields.zip(datatype.asInstanceOf[StructType].fields).forall { case (x1, x2) => x1.name == x2.name }) {
      true
    } else {
      false
    }
  }
}
