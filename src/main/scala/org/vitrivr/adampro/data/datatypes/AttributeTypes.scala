package org.vitrivr.adampro.data.datatypes

import org.apache.spark.sql.types
import org.apache.spark.sql.types.{ArrayType, DataType}
import org.vitrivr.adampro.data.datatypes.gis.{GeographyWrapper, GeometryWrapper}
import org.vitrivr.adampro.data.datatypes.vector.{Bit64VectorWrapper, SparseVectorWrapper, Vector}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object AttributeTypes {
  //TODO: clean up all datatypes and wrappers

  sealed abstract class AttributeType(val name: String, val datatype: DataType, val wrapper : Option[Wrapper] = None) extends Serializable {
    def equals(other: AttributeType): Boolean = other.name.equals(name)
  }

  case object AUTOTYPE extends AttributeType("auto", types.LongType)

  case object INTTYPE extends AttributeType("integer", types.IntegerType)

  case object LONGTYPE extends AttributeType("long", types.LongType)

  case object FLOATTYPE extends AttributeType("float", types.FloatType)

  case object DOUBLETYPE extends AttributeType("double", types.DoubleType)

  case object STRINGTYPE extends AttributeType("string", types.StringType)

  case object TEXTTYPE extends AttributeType("text", types.StringType)

  case object BOOLEANTYPE extends AttributeType("boolean", types.BooleanType)

  case object VECTORTYPE extends AttributeType("vector", ArrayType(Vector.VectorBaseSparkType, false))

  case object SPARSEVECTORTYPE extends AttributeType("sparsevector", SparseVectorWrapper.datatype, Some(SparseVectorWrapper))

  case object BIT64VECTORTYPE extends AttributeType("bit64vector", Bit64VectorWrapper.datatype, Some(Bit64VectorWrapper))

  case object BYTESVECTORTYPE extends AttributeType("bytesvector", types.BinaryType)

  case object GEOMETRYTYPE extends AttributeType("geometry", GeometryWrapper.datatype, Some(GeometryWrapper))

  case object GEOGRAPHYTYPE extends AttributeType("geography", GeographyWrapper.datatype, Some(GeographyWrapper))

  case object UNRECOGNIZEDTYPE extends AttributeType("", types.NullType)

  /**
    *
    */
  val values = Seq(AUTOTYPE, INTTYPE, LONGTYPE, FLOATTYPE, DOUBLETYPE, STRINGTYPE, TEXTTYPE, BOOLEANTYPE, VECTORTYPE, SPARSEVECTORTYPE, BIT64VECTORTYPE, BYTESVECTORTYPE, GEOMETRYTYPE, GEOGRAPHYTYPE)

  /**
    *
    * @param s
    * @return
    */
  implicit def fromString(s: String): AttributeType = values.filter(x => x.name == s).head

  /**
    *
    * @param d
    * @return
    */
  implicit def fromDataType(d: DataType): AttributeType = {
    val wrappedType = values.filter(_.wrapper.isDefined).filter(_.wrapper.get.fitsType(d)).headOption

    if(wrappedType.isDefined){
      //complex type
      wrappedType.get
    } else {
      //simple type
      values.filterNot(p => p == AUTOTYPE).filter(x => d == x.datatype).head
    }
  }
}