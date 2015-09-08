package ch.unibas.dmi.dbis.adam.storage.catalog

import slick.driver.H2Driver.api._
import slick.lifted.ForeignKeyQuery


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class TablesCatalog(tag: Tag) extends Table[(String)](tag, "__ADAMTWO_TABLES") {
  def tablename = column[String]("TABLENAME", O.PrimaryKey)
  def * = (tablename)
}

class IndexesCatalog(tag: Tag) extends Table[(String, String, String, String)](tag, "__ADAMTWO_INDEXES") {
  def indexname = column[String]("INDEXNAME", O.PrimaryKey)
  def tablename = column[String]("TABLENAME")
  def indextypename = column[String]("INDEXTYPENAME")
  def indexmeta = column[String]("INDEXMETA")

  def * = (indexname, tablename, indextypename, indexmeta)

  def supplier: ForeignKeyQuery[TablesCatalog, (String)] =
    foreignKey("TABLENAME", tablename, TableQuery[TablesCatalog])(_.tablename)
}

object Catalog {
  def apply() = List(
    ("__ADAMTWO_TABLES", TableQuery[TablesCatalog]),
    ("__ADAMTWO_INDEXES", TableQuery[IndexesCatalog])
  )
}