package org.vitrivr.adampro.datatypes.feature

import breeze.linalg.{DenseVector, SparseVector, Vector}

/**
  * adamtwo
  *
  * Feature data type.
  *
  * Ivan Giangreco
  * August 2015
  */
object Feature {
  //type definition
  type VectorBase = Float
  type FeatureVector = Vector[VectorBase]
  type DenseFeatureVector = DenseVector[VectorBase]
  type SparseFeatureVector = SparseVector[VectorBase]

  //TODO: possibly support int feature vectors, too


  //conversions
  implicit def conv_stored2vector(value: Seq[VectorBase]): DenseFeatureVector = new DenseVector[Float](value.toArray)

  implicit def conv_stored2vector(value: (Seq[Int], Seq[VectorBase], Int)): SparseFeatureVector = new SparseVector(value._1.toArray, value._2.toArray, value._3)

  implicit def conv_vector2stored(value: FeatureVector): Seq[VectorBase] = value.toArray

  implicit def conv_str2stored(value: String): Seq[VectorBase] = {
    require(value.length > 3)
    value.substring(1, value.length - 2).split(",").map(_.toDouble).map(_.toFloat).toSeq
  }

  implicit def conv_str2vector(value: String): FeatureVector = new DenseVector[Float](conv_str2stored(value).toArray)

  implicit def conv_double2vectorBase(value: Double): Float = value.toFloat

  implicit def conv_doublestored2floatstored(value: Seq[Double]): Seq[VectorBase] = value.map(_.toFloat)

  implicit def conv_doublevector2floatvector(value: DenseVector[Double]): FeatureVector = new DenseVector[Float](value.data.map(_.toFloat))
}





