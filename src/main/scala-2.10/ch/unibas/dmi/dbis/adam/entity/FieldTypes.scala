package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import org.apache.spark.sql.types
import org.apache.spark.sql.types.DataType

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldTypes {
  sealed abstract class FieldType(val name : String, val datatype : DataType) extends Serializable

  case object INTTYPE extends FieldType("integer", types.IntegerType)
  case object LONGTYPE extends FieldType("long", types.LongType)
  case object FLOATTYPE extends FieldType("float", types.FloatType)
  case object DOUBLETYPE extends FieldType("double", types.DoubleType)
  case object STRINGTYPE extends FieldType("string", types.StringType)
  case object BOOLEANTYPE extends FieldType("boolean", types.BooleanType)
  case object FEATURETYPE extends FieldType("feature", new FeatureVectorWrapperUDT)
  case object UNRECOGNIZEDTYPE extends FieldType("", types.NullType)
  case object AUTOTYPE extends FieldType("auto", types.LongType)

  def values = Seq(INTTYPE, LONGTYPE, FLOATTYPE, DOUBLETYPE, STRINGTYPE, BOOLEANTYPE, FEATURETYPE, AUTOTYPE)

  def fromString(s : String): FieldType = values.filter(x => x.name == s).head
  def fromDataType(d : DataType) : FieldType = values.filter(x => x.datatype == d).head
}