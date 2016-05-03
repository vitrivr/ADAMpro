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
  sealed abstract class FieldType(val datatype : DataType)

  case object INTTYPE extends FieldType(types.IntegerType)
  case object LONGTYPE extends FieldType(types.LongType)
  case object FLOATTYPE extends FieldType(types.FloatType)
  case object DOUBLETYPE extends FieldType(types.DoubleType)
  case object STRINGTYPE extends FieldType(types.StringType)
  case object BOOLEANTYPE extends FieldType(types.BooleanType)
  case object FEATURETYPE extends FieldType(new FeatureVectorWrapperUDT)
  case object UNRECOGNIZEDTYPE extends FieldType(types.NullType)
}