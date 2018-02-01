package org.vitrivr.adampro.data.datatypes.vector

import org.apache.spark.sql.types.{ArrayType, ByteType, DataType}
import org.vitrivr.adampro.data.datatypes.Wrapper

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * February 2018
  */
object ByteVectorWrapper extends Wrapper {
  /**
    *
    * @param d
    * @return
    */
  def fitsType(d: DataType) = {
    if(d.isInstanceOf[ArrayType] &&
      d.asInstanceOf[ArrayType].elementType == ByteType){
      true
    } else {
      false
    }
  }

  /**
    *
    * @return
    */
  override def datatype: DataType = ArrayType(ByteType)
}