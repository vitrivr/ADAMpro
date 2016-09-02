package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.utils.Logging

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object StorageHandlerRegistry extends Logging {
  val handlers = mutable.Map[String, StorageHandler]()

  /**
    *
    * @param name
    * @return
    */
  def apply(name: Option[String]): Option[StorageHandler] = {
    if (name.isDefined) {
      apply(name.get)
    } else {
      None
    }
  }

  /**
    *
    * @param name
    * @return
    */
  def apply(name: String): Option[StorageHandler] = handlers.get(name)

  /**
    *
    * @param name
    * @param fieldtype
    */
  def getOrElse(name: Option[String], fieldtype: FieldType): Option[StorageHandler] = {
    var result: Option[StorageHandler] = None

    if (result.isEmpty) {
      //use lookup
      result = apply(name)
    }

    if (result.isEmpty) {
      //try fallback: specializes
      result = handlers.values.filter(_.specializes.contains(fieldtype)).headOption
    }

    if (result.isEmpty) {
      //try fallback: supports
      result = handlers.values.filter(_.supports.contains(fieldtype)).headOption
    }

    if (result.isEmpty) {
      //no handler registered
      log.error("no suitable storage handler found in registry")
      throw new Exception("no suitable storage handler found in registry for " + fieldtype.name)
    } else {
      result
    }
  }

  /**
    *
    * @param handler
    */
  def register(handler: StorageHandler): Unit = {
    handlers += handler.name -> handler
  }

  /**
    *
    * @param name
    * @param handler
    */
  def register(name : String, handler: StorageHandler): Unit = {
    handlers += name -> handler
  }

  /**
    *
    * @param name
    */
  def unregister(name: String): Unit = {
    handlers -= name
  }
}
