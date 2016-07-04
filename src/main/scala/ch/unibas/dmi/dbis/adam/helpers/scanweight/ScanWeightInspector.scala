package ch.unibas.dmi.dbis.adam.helpers.scanweight

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.index.Index

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object ScanWeightInspector {
  /**
    * Returns the scan weight for the given entity.
    *
    * @param entity
    * @param attribute
    * @return
    */
  def apply(entity : Entity, attribute : String) = CatalogOperator.getEntityScanWeight(entity.entityname, attribute)

  /**
    * Sets the scan weight for the given entity.
    *
    * @param entity
    * @param attribute
    * @param weight
    * @return
    */
  def set(entity : Entity, attribute : String, weight : Float) = CatalogOperator.setEntityScanWeight(entity.entityname, attribute, Some(weight))

  /**
    * Returns the scan weight for the given index.
    *
    * @param index
    * @return
    */
  def apply(index : Index) = CatalogOperator.getIndexScanWeight(index.indexname)

  /**
    * Sets the scan weight for the given index.
    *
    * @param index
    * @param weight
    * @return
    */
  def set(index : Index, weight : Float) = CatalogOperator.setIndexScanWeight(index.indexname, Some(weight))
}
