package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.handler.{HandlerCatalog, Handler}

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
case class AttributeDefinition(name: String, fieldtype: FieldType, pk: Boolean = false, unique: Boolean = false, indexed: Boolean = false, var handlerName : Option[String] = None, var path : Option[String] = None){
  lazy val handler : Handler = {
    if(handlerName.isEmpty){
      fieldtype.defaultHandler
    } else {
      HandlerCatalog.apply(handlerName.get).get
    }
  }
}

