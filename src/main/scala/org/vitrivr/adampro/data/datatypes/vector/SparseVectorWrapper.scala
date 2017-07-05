package org.vitrivr.adampro.data.datatypes.vector

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.vitrivr.adampro.data.datatypes.Wrapper
import org.vitrivr.adampro.data.datatypes.gis.GeometryWrapper
import org.vitrivr.adampro.data.datatypes.vector.Vector.{DenseMathVector, MathVector, SparseMathVector, VectorBase}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
case class SparseVectorWrapper(index: Seq[Int], data: Seq[VectorBase], length: Int) {
  /**
    *
    * @return
    */
  def toRow() = SparseVectorWrapper.toRow(index, data, length)
}

object SparseVectorWrapper extends Wrapper {
  val datatype: DataType = StructType(Seq(
    StructField("index", ArrayType(IntegerType)),
    StructField("data", ArrayType(Vector.VectorBaseSparkType)),
    StructField("length", IntegerType)
  ))

  /**
    *
    * @param vec
    * @return
    */
  def apply(vec : SparseMathVector): SparseVectorWrapper = SparseVectorWrapper(vec.index, vec.data, vec.length)

  /**
    *
    * @param index
    * @param data
    * @param length
    * @return
    */
  def toRow(index: Seq[Int], data: Seq[VectorBase], length: Int) = {
    Row(index, data, length)
  }

  /**
    *
    * @return
    */
  def emptyRow = toRow(Seq(), Seq(), 0)


  /**
    *
    * @param r
    */
  def fromRow(r : Row): SparseVectorWrapper = SparseVectorWrapper(r.getAs[Seq[Int]]("index").toArray, r.getAs[Seq[VectorBase]]("data").toArray, r.getAs[Int]("length"))


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