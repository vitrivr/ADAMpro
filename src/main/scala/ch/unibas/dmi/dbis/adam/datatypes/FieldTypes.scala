package ch.unibas.dmi.dbis.adam.datatypes

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.datatypes.gis.{GeographyWrapperUDT, GeometryWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{ArrayType, DataType}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object FieldTypes {

  sealed abstract class FieldType(val name: String, val pk: Boolean, val datatype: DataType) extends Serializable {
    def equals(other: FieldType): Boolean = other.name.equals(name)
  }

  case object AUTOTYPE extends FieldType("auto", true, types.LongType)

  case object SERIALTYPE extends FieldType("serial", true, types.LongType) {
    /**
      *
      * @param entityname name of entity
      * @param attribute  attribute
      * @return
      */
    def getNext(entityname: EntityName, attribute: String): Long = {
      CatalogOperator.getAndUpdateAttributeOption(entityname, attribute, attribute + "-serial", "0", (s: String) => ((s.toLong + 1).toString))
        .get.get(attribute + "-serial").get.toLong
    }
  }

  case object INTTYPE extends FieldType("integer", true, types.IntegerType)

  case object LONGTYPE extends FieldType("long", true, types.LongType)

  case object FLOATTYPE extends FieldType("float", false, types.FloatType)

  case object DOUBLETYPE extends FieldType("double", false, types.DoubleType)

  case object STRINGTYPE extends FieldType("string", true, types.StringType)

  case object TEXTTYPE extends FieldType("text", false, types.StringType)

  case object BOOLEANTYPE extends FieldType("boolean", false, types.BooleanType)

  case object FEATURETYPE extends FieldType("feature", false, new FeatureVectorWrapperUDT)

  case object GEOMETRYTYPE extends FieldType("geometry", false, new GeometryWrapperUDT)

  case object GEOGRAPHYTYPE extends FieldType("geography", false, new GeographyWrapperUDT)

  case object UNRECOGNIZEDTYPE extends FieldType("", false, types.NullType)


  //TODO: only add to values if handler is available
  def values = Seq(INTTYPE, LONGTYPE, FLOATTYPE, DOUBLETYPE, STRINGTYPE, TEXTTYPE, BOOLEANTYPE, FEATURETYPE, GEOMETRYTYPE, GEOGRAPHYTYPE, AUTOTYPE, SERIALTYPE)

  implicit def fromString(s: String): FieldType = values.filter(x => x.name == s).head

  implicit def fromDataType(d: DataType): FieldType = {
    val suggestion = values.filter(x => d == x.datatype)
    if (d.isInstanceOf[ArrayType]) {
      FEATURETYPE
    } else {
      suggestion.head
    }
  }
}