package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * ADAMpro
  *
  * Catalog to store the indexes.
  *
  * Ivan Giangreco
  * June 2016
  */
private[catalog] class IndexCatalog(tag: Tag) extends Table[(String, String, String, String, Array[Byte], Boolean)](tag, Some(CatalogOperator.SCHEMA), "ap_index") {
  def indexname = column[String]("indexname", O.PrimaryKey)

  def entityname = column[String]("entityname")

  def attribute = column[String]("attribute")

  def indextypename = column[String]("indextypename")

  def meta = column[Array[Byte]]("meta")

  def isUpToDate = column[Boolean]("uptodate")

  /**
    * Special fields
    */
  def * = (indexname, entityname, attribute, indextypename, meta, isUpToDate)

  def entity = foreignKey("index_entity_fk", entityname, TableQuery[EntityCatalog])(_.entityname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)

  def options = foreignKey("index_options_fk", indexname, TableQuery[IndexOptionsCatalog])(_.indexname)

  def weights = foreignKey("index_weight_fk", indexname, TableQuery[IndexWeightCatalog])(_.indexname)
}