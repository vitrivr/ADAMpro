package org.vitrivr.adampro.datatypes

import org.apache.spark.sql.types
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.vitrivr.adampro.datatypes.gis.{GeographyWrapper, GeometryWrapper}
import org.vitrivr.adampro.datatypes.vector.{DenseVector, SparseVectorWrapper, Vector}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldTypes {

  sealed abstract class FieldType(val name: String, val datatype: DataType, val wrapper : Option[Wrapper] = None) extends Serializable {
    def equals(other: FieldType): Boolean = other.name.equals(name)
  }

  case object INTTYPE extends FieldType("integer", types.IntegerType)

  case object LONGTYPE extends FieldType("long", types.LongType)

  case object FLOATTYPE extends FieldType("float", types.FloatType)

  case object DOUBLETYPE extends FieldType("double", types.DoubleType)

  case object STRINGTYPE extends FieldType("string", types.StringType)

  case object TEXTTYPE extends FieldType("text", types.StringType)

  case object BOOLEANTYPE extends FieldType("boolean", types.BooleanType)

  case object VECTORTYPE extends FieldType("vector", ArrayType(Vector.VectorBaseSparkType, false))

  case object SPARSEVECTORTYPE extends FieldType("sparsevector", SparseVectorWrapper.datatype, Some(SparseVectorWrapper))

  case object GEOMETRYTYPE extends FieldType("geometry", GeometryWrapper.datatype, Some(GeometryWrapper))

  case object GEOGRAPHYTYPE extends FieldType("geography", GeographyWrapper.datatype, Some(GeographyWrapper))

  case object UNRECOGNIZEDTYPE extends FieldType("", types.NullType)

  /**
    *
    */
  val values = Seq(INTTYPE, LONGTYPE, FLOATTYPE, DOUBLETYPE, STRINGTYPE, TEXTTYPE, BOOLEANTYPE, VECTORTYPE, SPARSEVECTORTYPE, GEOMETRYTYPE, GEOGRAPHYTYPE)

  /**
    *
    * @param s
    * @return
    */
  implicit def fromString(s: String): FieldType = values.filter(x => x.name == s).head

  /**
    *
    * @param d
    * @return
    */
  implicit def fromDataType(d: DataType): FieldType = {
    val wrappedType = values.filter(_.wrapper.isDefined).filter(_.wrapper.get.fitsType(d)).headOption

    if(wrappedType.isDefined){
      //complex type
      wrappedType.get
    } else {
      //simple type
      values.filter(x => d == x.datatype).head
    }
  }
}