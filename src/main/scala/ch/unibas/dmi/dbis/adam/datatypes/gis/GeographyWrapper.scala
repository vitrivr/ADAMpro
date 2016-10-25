package ch.unibas.dmi.dbis.adam.datatypes.gis

import org.apache.spark.sql.types.{DataTypes, DataType, UserDefinedType, SQLUserDefinedType}
import org.postgis.PGgeometry

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
@SQLUserDefinedType(udt = classOf[GeographyWrapperUDT])
case class GeographyWrapper(desc : String) extends PGgeometry(desc) {}


class GeographyWrapperUDT extends UserDefinedType[GeographyWrapper] {
  override def sqlType: DataType = DataTypes.StringType

  override def serialize(obj: Any): Any = obj.asInstanceOf[GeographyWrapper].getValue

  override def userClass: Class[GeographyWrapper] = classOf[GeographyWrapper]
  override def asNullable: GeographyWrapperUDT = this

  override def deserialize(datum: Any): GeographyWrapper = new GeographyWrapper(datum.toString)
}
