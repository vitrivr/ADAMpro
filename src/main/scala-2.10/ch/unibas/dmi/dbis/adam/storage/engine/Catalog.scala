package ch.unibas.dmi.dbis.adam.storage.engine

import slick.driver.H2Driver.api._
import slick.lifted.ForeignKeyQuery


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
private[engine] class EntitiesCatalog(tag: Tag) extends Table[(String, Int, Boolean)](tag, "ADAMTWO_ENTITIES") {
  def entityname = column[String]("ENTITYNAME", O.PrimaryKey)
  def featurelength = column[Int]("FEATURELENGTH")
  def hasMeta = column[Boolean]("HASMETA")
  def * = (entityname, featurelength, hasMeta)
}

private[engine] class IndexesCatalog(tag: Tag) extends Table[(String, String, String, String, String)](tag, "ADAMTWO_INDEXES") {
  def indexname = column[String]("INDEXNAME", O.PrimaryKey)
  def entityname = column[String]("ENTITYNAME")
  def indextypename = column[String]("INDEXTYPENAME")
  def indexpath = column[String]("INDEXPATH")
  def indexmetapath = column[String]("INDEXMETAPATH")

  def * = (indexname, entityname, indextypename, indexpath, indexmetapath)

  def supplier: ForeignKeyQuery[EntitiesCatalog, (String, Int, Boolean)] =
    foreignKey("ENTITYNAME", entityname, TableQuery[EntitiesCatalog])(_.entityname)
}

private[engine] object Catalog {
  def apply() = List(
    ("ADAMTWO_ENTITIES", TableQuery[EntitiesCatalog]),
    ("ADAMTWO_INDEXES", TableQuery[IndexesCatalog])
  )
}