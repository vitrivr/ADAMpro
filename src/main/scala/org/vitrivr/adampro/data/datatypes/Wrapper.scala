package org.vitrivr.adampro.data.datatypes

import org.apache.spark.sql.types.DataType

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
trait Wrapper {
  /**
    *
    * @return
    */
  def datatype : DataType

  /**
    *
    * @param d
    * @return
    */
  def fitsType(d: DataType) : Boolean
}
