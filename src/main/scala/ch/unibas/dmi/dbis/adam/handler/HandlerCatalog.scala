package ch.unibas.dmi.dbis.adam.handler

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object HandlerCatalog {
  val handlers = mutable.Map[String, Handler] ()

  def apply(name : String) : Option[Handler] = handlers.get(name)

  def register(handler : Handler): Unit = {
    handlers += handler.name -> handler
  }

  def unregister(name : String): Unit = {
    handlers -= name
  }
}
