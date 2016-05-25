package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import FieldTypes.FieldType

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  *
  * @param name
  * @param fieldtype
  * @param pk
  * @param unique
  * @param indexed
  */
case class FieldDefinition(name: String, fieldtype: FieldType, pk: Boolean = false, unique: Boolean = false, indexed: Boolean = false)

