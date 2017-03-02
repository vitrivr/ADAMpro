package org.vitrivr.adampro.datatypes.vector

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.DataTypes
import scala.util.Random

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object Vector {
  type VectorBase = Float
  val VectorBaseSparkType = DataTypes.FloatType

  val zeroValue : VectorBase = 0.toFloat
  val minValue : VectorBase = Float.MinValue
  val maxValue : VectorBase = Float.MaxValue
  def nextRandom() : VectorBase = Random.nextFloat()

  type DenseRawVector = Seq[VectorBase]
  type SparseRawVector = SparseVectorWrapper

  type DenseSparkVector = DenseRawVector
  type SparseSparkVector = Row

  type MathVector = BV[VectorBase]

  type DenseMathVector = BDV[VectorBase]
  type SparseMathVector = BSV[VectorBase]

  def conv_draw2vec(v: DenseRawVector): DenseMathVector = BDV.apply(v.toArray)
  def conv_sraw2vec(v: SparseRawVector): SparseMathVector = new BSV(v.index.toArray, v.data.toArray, v.length)
  def conv_sraw2vec(index: Seq[Int], data: Seq[VectorBase], length: Int): SparseMathVector = new BSV(index.toArray, data.toArray, length)
  def conv_dspark2vec(v: DenseSparkVector): DenseMathVector = BDV.apply(v.toArray)
  def conv_sspark2vec(v: SparseSparkVector): SparseMathVector = conv_sraw2vec(SparseVectorWrapper.fromRow(v))
  def conv_array2vec(v: Array[VectorBase]): DenseMathVector = BDV.apply(v)

  def conv_vec2dspark(v: DenseMathVector): DenseSparkVector = v.data
  def conv_vec2sspark(v: SparseMathVector): SparseSparkVector = SparseVectorWrapper(v.index, v.data, v.length).toRow()


  def conv_int2vb(v: Int): VectorBase = v.toFloat
  def conv_float2vb(v: Float): VectorBase = v
  def conv_double2vb(v: Double): VectorBase = v.toFloat
  def conv_str2vb(v : String): VectorBase = v.toFloat
}

