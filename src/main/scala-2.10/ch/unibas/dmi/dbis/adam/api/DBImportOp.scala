package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object DBImportOp {
  def apply(url : String, port : Int, database : String, user : String, password : String, tablename: EntityName, columns : String) : Unit = {
    //TODO create function to import data from database, see GenerateDataOp
  }
}
