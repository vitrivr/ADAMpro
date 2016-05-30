package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import FieldTypes.FieldType

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  *
  * @param name      name of attribute
  * @param fieldtype type of field
  * @param pk        is primary key
  * @param unique    is unique
  * @param indexed   is indexed
  */
case class AttributeDefinition(name: String, fieldtype: FieldType, pk: Boolean = false, unique: Boolean = false, indexed: Boolean = false)

