package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object ImportOp {
  /**
   *
   * @param tablename
   * @param csv
   * @param separator
   * @param ignoreErrors
   */
  def apply(tablename : EntityName, csv : Seq[String], separator : String = "\t", ignoreErrors : Boolean = true) : Unit = {
    val data = csv.filter(line => line.trim.length != 0)
      .filter(line => filterLine(line, separator, ignoreErrors))
      .map(line => line.split(separator))
      .map(line => (line(0).toLong, line(1).substring(1, line(1).length - 2).split(",").map(_.toFloat).toSeq)) //TODO: make this a tuple

    import SparkStartup.sqlContext.implicits._
    Entity.insertData(tablename, data.toDF())
  }

  /**
   *
   * @param tablename
   */
  def apply(tablename : EntityName, data : DataFrame) : Unit = {
    Entity.insertData(tablename, data)
  }

  /**
   *
   * @param line
   * @param separator
   * @param ignoreErrors
   * @return
   */
  def filterLine(line : String, separator : String, ignoreErrors : Boolean): Boolean ={
    if(!ignoreErrors){
      return true
    }

    if(!line.contains(separator)) return false

    val splitted = line.split(separator)
    if(splitted.length != 2) return false

    return true
  }
}
