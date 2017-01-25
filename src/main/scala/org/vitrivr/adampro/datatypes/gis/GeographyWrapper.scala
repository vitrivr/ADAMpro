package org.vitrivr.adampro.datatypes.gis

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.postgis.PGgeometry
import org.vitrivr.adampro.datatypes.Wrapper

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
case class GeographyWrapper(desc : String) extends PGgeometry(desc) {
  def toRow() = GeographyWrapper.toRow(desc)
}


object GeographyWrapper extends Wrapper{
  private val FIELD_NAME = "geography_desc"

  val datatype: DataType = StructType(Seq(
    StructField(FIELD_NAME, StringType)
  ))

  /**
    *
    * @param desc
    * @return
    */
  def toRow(desc : String) =  Row(desc)

  /**
    *
    * @return
    */
  def emptyRow = toRow("")

  /**
    *
    * @param r
    */
  def fromRow(r : Row): GeographyWrapper = GeographyWrapper(r.getString(0))

  /**
    *
    * @param d
    * @return
    */
  def fitsType(d: DataType) = {
    if(d.isInstanceOf[StructType] &&
      d.asInstanceOf[StructType].fields.apply(0).name == FIELD_NAME){
      true
    } else {
      false
    }
  }
}
