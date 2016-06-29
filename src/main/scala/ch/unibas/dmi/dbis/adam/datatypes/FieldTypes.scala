package ch.unibas.dmi.dbis.adam.datatypes

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.handler.{FeatureDatabaseHandler, Handler, RelationalDatabaseHandler, TextRetrievalHandler}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{ArrayType, DataType}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldTypes {

  sealed abstract class FieldType(val name: String, val pk : Boolean, val datatype: DataType, val defaultHandler : Handler) extends Serializable {
    def equals(other: FieldType): Boolean = other.name.equals(name)
  }

  case object AUTOTYPE extends FieldType("auto", true, types.LongType, RelationalDatabaseHandler)

  case object INTTYPE extends FieldType("integer", true, types.IntegerType, RelationalDatabaseHandler)

  case object LONGTYPE extends FieldType("long", true, types.LongType, RelationalDatabaseHandler)

  case object FLOATTYPE extends FieldType("float", false, types.FloatType, RelationalDatabaseHandler)

  case object DOUBLETYPE extends FieldType("double", false, types.DoubleType, RelationalDatabaseHandler)

  case object STRINGTYPE extends FieldType("string", true, types.StringType, RelationalDatabaseHandler)

  case object TEXTTYPE extends FieldType("text", false, types.StringType, TextRetrievalHandler)

  case object BOOLEANTYPE extends FieldType("boolean", false, types.BooleanType, RelationalDatabaseHandler)

  case object FEATURETYPE extends FieldType("feature", false, new FeatureVectorWrapperUDT, FeatureDatabaseHandler)

  case object UNRECOGNIZEDTYPE extends FieldType("", false, types.NullType, null)

  def values = Seq(INTTYPE, LONGTYPE, FLOATTYPE, DOUBLETYPE, STRINGTYPE, TEXTTYPE, BOOLEANTYPE, FEATURETYPE, AUTOTYPE)

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