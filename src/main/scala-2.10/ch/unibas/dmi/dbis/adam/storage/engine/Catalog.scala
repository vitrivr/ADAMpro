package ch.unibas.dmi.dbis.adam.storage.engine

import slick.driver.H2Driver.api._
import slick.lifted.ForeignKeyQuery


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[engine] class EntitiesCatalog(tag: Tag) extends Table[(String, Boolean)](tag, "__ADAMTWO_ENTITIES") {
  def entityname = column[String]("ENTITYNAME", O.PrimaryKey)
  def hasMeta = column[Boolean]("HASMETA")
  def * = (entityname, hasMeta)
}

private[engine] class IndexesCatalog(tag: Tag) extends Table[(String, String, String, String)](tag, "__ADAMTWO_INDEXES") {
  def indexname = column[String]("INDEXNAME", O.PrimaryKey)
  def entityname = column[String]("ENTITYNAME")
  def indextypename = column[String]("INDEXTYPENAME")
  def indexmeta = column[String]("INDEXMETA")

  def * = (indexname, entityname, indextypename, indexmeta)

  def supplier: ForeignKeyQuery[EntitiesCatalog, (String, Boolean)] =
    foreignKey("ENTITYNAME", entityname, TableQuery[EntitiesCatalog])(_.entityname)
}

private[engine] object Catalog {
  def apply() = List(
    ("__ADAMTWO_ENTITIES", TableQuery[EntitiesCatalog]),
    ("__ADAMTWO_INDEXES", TableQuery[IndexesCatalog])
  )
}