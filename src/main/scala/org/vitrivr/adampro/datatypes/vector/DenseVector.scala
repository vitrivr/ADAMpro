package org.vitrivr.adampro.datatypes.vector

import org.apache.spark.sql.types.{ArrayType, DataType, StructType}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
object DenseVector {
  /**
    *
    * @param d
    * @return
    */
  def fitsType(d: DataType) = {
    if(d.isInstanceOf[ArrayType] &&
      d.asInstanceOf[ArrayType].elementType == Vector.VectorBaseSparkType){
      true
    } else {
      false
    }
  }
}
