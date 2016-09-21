package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.storage.engine.Engine
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
    */
  def get(name: String): Option[StorageHandler] = {
    var result: Option[StorageHandler] = None

    if (result.isEmpty) {
      //use lookup
      result = apply(name)
    }

    if (result.isEmpty) {
      //no handler registered
      log.error("no suitable storage handler found in registry")
      throw new Exception("no suitable storage handler found in registry for " + name)
    } else {
      result
    }
  }

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
      throw new GeneralAdamException("no suitable storage handler found in registry for " + fieldtype.name)
    } else {
      result
    }
  }

  /**
    *
    * @param configname
    */
  def register(configname: String): Unit = {
    try {
      val props: Map[String, String] = AdamConfig.getStorageProperties(configname).toMap

      val engineName = props.get("engine")
      if (engineName.isEmpty) {
        throw new Exception("no suitable engine entry found in config for " + configname)
      }

      val constructor = Class.forName(classOf[Engine].getPackage.getName + "." + engineName.get).getConstructor(classOf[Map[_, _]])

      val engine = constructor.newInstance(props).asInstanceOf[Engine]
      val handler = new StorageHandler(engine)

      val name = props.getOrElse("storagename", handler.name)

      register(name, handler)
    } catch {
      case e: Exception => log.error("error in registering handler for " + configname, e)
    }
  }

  /**
    *
    * @param handler
    */
  def register(handler: StorageHandler): Unit = {
    register(handler.name, handler)
  }

  /**
    *
    * @param name
    * @param handler
    */
  def register(name: String, handler: StorageHandler): Unit = {
    if(handlers.contains(name)){
      log.error("handler with name " + name + " exists already")
    } else {
      handlers += name -> handler
    }
  }

  /**
    *
    * @param name
    */
  def unregister(name: String): Unit = {
    handlers -= name
  }
}
