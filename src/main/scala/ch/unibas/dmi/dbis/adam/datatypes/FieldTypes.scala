package ch.unibas.dmi.dbis.adam.datatypes

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{ArrayType, DataType}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldTypes {

  sealed abstract class FieldType(val name: String, val datatype: DataType) extends Serializable {
    def equals(other: FieldType): Boolean = other.name.equals(name)
  }

  case object AUTOTYPE extends FieldType("auto", types.LongType)

  case object INTTYPE extends FieldType("integer", types.IntegerType)

  case object LONGTYPE extends FieldType("long", types.LongType)

  case object FLOATTYPE extends FieldType("float", types.FloatType)

  case object DOUBLETYPE extends FieldType("double", types.DoubleType)

  case object STRINGTYPE extends FieldType("string", types.StringType)

  case object BOOLEANTYPE extends FieldType("boolean", types.BooleanType)

  case object FEATURETYPE extends FieldType("feature", new FeatureVectorWrapperUDT)

  case object UNRECOGNIZEDTYPE extends FieldType("", types.NullType)

  def values = Seq(INTTYPE, LONGTYPE, FLOATTYPE, DOUBLETYPE, STRINGTYPE, BOOLEANTYPE, FEATURETYPE, AUTOTYPE)

  implicit def fromString(s: String): FieldType = values.filter(x => x.name == s).head

  implicit def fromDataType(d: DataType): FieldType = {
    val suggestion = values.filter(x => d == x.datatype)
    if (d .isInstanceOf[ArrayType]) {
      FEATURETYPE
    } else {
      suggestion.head
    }
  }
}