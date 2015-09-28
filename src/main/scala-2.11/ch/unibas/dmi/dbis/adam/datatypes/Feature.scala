package ch.unibas.dmi.dbis.adam.datatypes

import breeze.linalg.DenseVector

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Feature {
  //type definition
  type VectorBase = Float
  type WorkingVector = DenseVector[VectorBase]
  type StoredVector = Seq[VectorBase]


  //conversions
  implicit def conv_stored2vector(value: StoredVector): WorkingVector = new DenseVector[Float](value.toArray)
  implicit def conv_vector2stored(value: WorkingVector): StoredVector = value.toArray

  implicit def conv_str2stored(value: String): StoredVector = {
   if(value.length <= 3){
     return Seq[Float]()
   }
    value.substring(1, value.length - 2).split(",").map(_.toDouble).map(_.toFloat).toSeq
  }

  implicit def conv_str2vector(value: String): WorkingVector =  conv_stored2vector(conv_str2stored(value))

  implicit def conv_double2base(value : Double): Float = value.toFloat

  implicit def conv_double2vectorBase(value : Double): Float = value.toFloat

  implicit def conv_doublestored2floatstored (value : Seq[Double]) : StoredVector = value.map(_.toFloat)

  implicit def conv_doublevector2floatvector (value : DenseVector[Double]) : WorkingVector = new DenseVector[Float](value.data.map(_.toFloat))
}

