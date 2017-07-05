package org.vitrivr.adampro.data.datatypes.vector

import org.apache.spark.sql.types.{ArrayType, DataType}
import org.vitrivr.adampro.data.datatypes.Wrapper
import org.vitrivr.adampro.data.datatypes.vector.Vector.VectorBaseSparkType

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
object DenseVectorWrapper extends Wrapper {
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

  /**
    *
    * @return
    */
  override def datatype: DataType = ArrayType(VectorBaseSparkType)
}
