package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.storage.handler.{StorageHandler, StorageHandlerRegistry}

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
case class AttributeDefinition(name: String, fieldtype: FieldType, pk: Boolean = false, unique: Boolean = false, indexed: Boolean = false, private val storagehandlername : Option[String] = None, params : Map[String, String] = Map()){
  /**
    * Returns the storage handler for the given attribute (it possibly uses a fallback, if no storagehandlername is specified by using the fieldtype)
    */
  lazy val storagehandler : Option[StorageHandler] = StorageHandlerRegistry.getOrElse(storagehandlername, fieldtype)
}

