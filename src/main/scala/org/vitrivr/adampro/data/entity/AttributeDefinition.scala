package org.vitrivr.adampro.data.entity

import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.AttributeTypes.AttributeType
import org.vitrivr.adampro.data.entity.Entity.AttributeName
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.storage.StorageHandler

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  *
  * @param name          name of attribute
  * @param attributeType type of attribute
  * @param storagehandlername
  * @param params
  */
case class AttributeDefinition(name: AttributeName, attributeType: AttributeType, storagehandlername: String, params: Map[String, String] = Map()) {
  def this(name: AttributeName, attributetype: AttributeType, params: Map[String, String])(implicit ac: SharedComponentContext) {
    this(name, attributetype, ac.storageManager.get(attributetype).get._1, params)
  }

  def this(name: AttributeName, attributetype: AttributeType)(implicit ac: SharedComponentContext) {
    this(name, attributetype, ac.storageManager.get(attributetype).get._1, Map[String, String]())
  }

  /**
    * is attribute primary key
    */
  @deprecated val pk: Boolean = (name == AttributeNames.internalIdColumnName)

  /**
    * Returns the storage handler for the given attribute (it possibly uses a fallback, if no storagehandlername is specified by using the fieldtype)
    */
  def storagehandler()(implicit ac: SharedComponentContext): StorageHandler = {
    val handler = ac.storageManager.get(storagehandlername)

    if (handler.isDefined) {
      handler.get
    } else {
      throw new GeneralAdamException("no handler found for " + storagehandlername)
    }
  }

  /**
    * Returns a map of properties to the entity. Useful for printing.
    */
  def propertiesMap: Map[String, String] = {
    val lb = ListBuffer[(String, String)]()

    lb.append("fieldtype" -> attributeType.name)
    lb.append("pk" -> pk.toString)

    if (!pk) {
      lb.append("storagehandler" -> storagehandlername)
    }

    lb.append("parameters" -> params.toString())

    lb.toMap
  }
}

