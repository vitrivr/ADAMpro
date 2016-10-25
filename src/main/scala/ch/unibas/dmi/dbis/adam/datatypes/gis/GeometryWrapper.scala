package ch.unibas.dmi.dbis.adam.datatypes.gis

import org.apache.spark.sql.types.{DataType, DataTypes, SQLUserDefinedType, UserDefinedType}
import org.postgis.PGgeometry

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
@SQLUserDefinedType(udt = classOf[GeometryWrapperUDT])
class GeometryWrapper(desc : String) extends PGgeometry(desc) {}


class GeometryWrapperUDT extends UserDefinedType[GeometryWrapper] {
  override def sqlType: DataType = DataTypes.StringType

  override def serialize(obj: Any): Any = obj.asInstanceOf[GeometryWrapper].getValue

  override def userClass: Class[GeometryWrapper] = classOf[GeometryWrapper]
  override def asNullable: GeometryWrapperUDT = this

  override def deserialize(datum: Any): GeometryWrapper = new GeometryWrapper(datum.toString)
}
