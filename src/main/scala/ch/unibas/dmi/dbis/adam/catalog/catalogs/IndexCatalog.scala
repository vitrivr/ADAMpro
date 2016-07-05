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
  def indexname = column[String]("index", O.PrimaryKey)

  def entityname = column[String]("entity")

  def attributename = column[String]("attribute")

  def indextypename = column[String]("indextype")

  def meta = column[Array[Byte]]("meta")

  def isUpToDate = column[Boolean]("uptodate")

  /**
    * Special fields
    */
  def * = (indexname, entityname, attributename, indextypename, meta, isUpToDate)

  def attribute = foreignKey("index_attribute_fk", (entityname, attributename), TableQuery[AttributeCatalog])(t => (t.entityname, t.attributename), onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)
}