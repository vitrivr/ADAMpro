package org.vitrivr.adampro.entity

import org.vitrivr.adampro.entity.Entity.EntityName

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
case class EntityNameHolder(originalName: String) {
  override def toString = cleanName(originalName)

  override def canEqual(a: Any) = a.isInstanceOf[EntityNameHolder] || a.isInstanceOf[String]

  override def equals(that: Any): Boolean =
    that match {
      case that: EntityNameHolder => that.canEqual(this) && toString.equals(that.toString)
      case that : String => that.canEqual(this) && originalName.equals(cleanName(that))
      case _ => false
    }

  override def hashCode: Int = originalName.hashCode

  /**
    *
    * @param str name of entity
    * @return
    */
  private def cleanName(str : String): String = str.replaceAll("[^A-Za-z0-9_-]", "").toLowerCase()
}

object EntityNameHolder {
  implicit def toString(name: EntityName): String = name.toString
  implicit def fromString(str: String): EntityName = EntityNameHolder(str)
}