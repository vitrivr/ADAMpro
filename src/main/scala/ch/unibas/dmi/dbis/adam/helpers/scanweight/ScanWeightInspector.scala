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

  def apply(entity : Entity, attribute : String) = CatalogOperator.getEntityScanWeight(entity.entityname, attribute)

  def set(entity : Entity, attribute : String, weight : Float) = CatalogOperator.setEntityScanWeight(entity.entityname, attribute, Some(weight))

  def apply(index : Index) = CatalogOperator.getIndexScanWeight(index.indexname)

  def set(index : Index, weight : Float) = CatalogOperator.setIndexScanWeight(index.indexname, Some(weight))
}
