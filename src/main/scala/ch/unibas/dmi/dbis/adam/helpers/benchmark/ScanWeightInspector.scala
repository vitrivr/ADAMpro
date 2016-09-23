package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object ScanWeightInspector {
  val DEFAULT_WEIGHT = 100.toFloat
  val SCANWEIGHT_OPTION_NAME = "scanning-weight"

  /**
    *
    * @param entity
    * @param attribute
    * @return
    */
  def apply(entity: Entity, attribute: String): Float = apply(entity.entityname, attribute)

  /**
    * Returns the scan weight for the given entity.
    *
    * @param entityname
    * @param attribute
    * @return
    */
  def apply(entityname: EntityName, attribute: String): Float = CatalogOperator.getAttributeOption(entityname, attribute, Some(SCANWEIGHT_OPTION_NAME)).get.getOrElse(SCANWEIGHT_OPTION_NAME, DEFAULT_WEIGHT.toString).toFloat

  /**
    * Sets the scan weight for the given entity.
    *
    * @param entity
    * @param attribute
    * @param weight
    * @return
    */
  def set(entity: Entity, attribute: String, weight: Float): Unit = set(entity.entityname, attribute, weight)

  /**
    * Sets the scan weight for the given entity.
    *
    * @param entityname
    * @param attribute
    * @param weight
    */
  def set(entityname: EntityName, attribute: String, weight: Float): Unit = CatalogOperator.updateAttributeOption(entityname, attribute, ScanWeightInspector.SCANWEIGHT_OPTION_NAME, weight.toString)

  /**
    * Returns the scan weight for the given index.
    *
    * @param index
    * @return
    */
  def apply(index: Index): Float = apply(index.indexname)

  /**
    *
    * @param indexname
    */
  def apply(indexname: IndexName): Float = CatalogOperator.getIndexOption(indexname, Some(SCANWEIGHT_OPTION_NAME)).get.getOrElse(SCANWEIGHT_OPTION_NAME, DEFAULT_WEIGHT.toString).toFloat

  /**
    *
    * @param index
    * @param weight
    * @return
    */
  def set(index: Index, weight: Float) : Unit = set(index.indexname, weight)

  /**
    * Sets the scan weight for the given index.
    *
    * @param indexname
    * @param weight
    * @return
    */
  def set(indexname: IndexName, weight: Float) : Unit = CatalogOperator.updateIndexOption(indexname, SCANWEIGHT_OPTION_NAME, weight.toString).get
}
