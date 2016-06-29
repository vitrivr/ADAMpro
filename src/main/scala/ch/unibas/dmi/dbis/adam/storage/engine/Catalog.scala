package ch.unibas.dmi.dbis.adam.storage.engine

import slick.driver.PostgresDriver.api._
import slick.lifted.ForeignKeyQuery


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
private[engine] class EntitiesCatalog(tag: Tag) extends Table[(String)](tag, "ADAMTWO_ENTITIES") {
  def entityname = column[String]("ENTITYNAME", O.PrimaryKey)

  def * = (entityname)
}

private[engine] class EntityFieldsCatalog(tag: Tag) extends Table[(String, String, Boolean, Boolean, Boolean, String, Int, Float, String, String)](tag, "ADAMTWO_FIELDS") {
  def fieldname = column[String]("FIELDNAME")

  def fieldtype = column[String]("FIELDTYPE")

  def pk = column[Boolean]("PRIMARYKEY")

  def unique = column[Boolean]("UNIQUE")

  def indexed = column[Boolean]("INDEXED")

  def entityname = column[String]("ENTITYNAME")

  def featurelength = column[Int]("FEATURELENGTH")

  def scanweight = column[Float]("WEIGHT")

  def handler = column[String]("HANDLER")

  def path = column[String]("PATH")

  def * = (fieldname, fieldtype, pk, unique, indexed, entityname, featurelength, scanweight, handler, path)

  def supplier: ForeignKeyQuery[EntitiesCatalog, (String)] =
    foreignKey("ENTITYNAME", entityname, TableQuery[EntitiesCatalog])(_.entityname)
}

private[engine] class IndexesCatalog(tag: Tag) extends Table[(String, String, String, String, String, Array[Byte], Boolean, Float)](tag, "ADAMTWO_INDEXES") {
  def indexname = column[String]("INDEXNAME", O.PrimaryKey)

  def fieldname = column[String]("FIELDNAME")

  def entityname = column[String]("ENTITYNAME")

  def indextypename = column[String]("INDEXTYPENAME")

  def indexpath = column[String]("INDEXPATH")

  def indexmeta = column[Array[Byte]]("INDEXMETA")

  def uptodate = column[Boolean]("ISUPTODATE")

  def scanweight = column[Float]("WEIGHT")

  def * = (indexname, entityname, fieldname, indextypename, indexpath, indexmeta, uptodate, scanweight)

  def supplier: ForeignKeyQuery[EntitiesCatalog, (String)] =
    foreignKey("ENTITYNAME", entityname, TableQuery[EntitiesCatalog])(_.entityname)
}

private[engine] object Catalog {
  def apply() = List(
    ("ADAMTWO_ENTITIES", TableQuery[EntitiesCatalog]),
    ("ADAMTWO_FIELDS", TableQuery[EntityFieldsCatalog]),
    ("ADAMTWO_INDEXES", TableQuery[IndexesCatalog])
  )
}