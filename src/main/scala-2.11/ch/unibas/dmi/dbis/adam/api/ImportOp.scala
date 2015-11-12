package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.VectorBase
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
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
   * @param entityname
   * @param csv
   * @param separator
   * @param ignoreErrors
   */
  def apply(entityname : EntityName, csv : Seq[String], separator : String = "\t", ignoreErrors : Boolean = true) : Unit = {
    val data = csv.filter(line => line.trim.length != 0)
      .filter(line => filterLine(line, separator, ignoreErrors))
      .map(line => line.split(separator))
      .map(line => (line(0).toLong, line(1).substring(1, line(1).length - 2).split(",").map(_.toFloat).toSeq)) //TODO: make this a tuple

    import SparkStartup.sqlContext.implicits._
    Entity.insertData(entityname, data.toDF())
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


  /**
   *
   * @param entityname
   * @param floatVectorData
   */
  def importDense(entityname : EntityName, floatVectorData : DataFrame) : Unit = {
    //assume schema [id: bigint, feature: array<float>]

    import SparkStartup.sqlContext.implicits._
    val  data = floatVectorData.map(r =>
      (r.getLong(0), new FeatureVectorWrapper(r.getSeq[VectorBase](1)))
    ).toDF()

    Entity.insertData(entityname, data)
  }

  /**
   *
   * @param entityname
   * @param floatVectorData
   * @param length
   */
  def importSparse(entityname : EntityName, floatVectorData : DataFrame, length : Int) : Unit = {
    //assume schema [id: bigint, index: array<int>, feature: array<float>]

    import SparkStartup.sqlContext.implicits._
    val  data = floatVectorData.map(r =>
      (r.getLong(0), new FeatureVectorWrapper(r.getSeq[Int](1), r.getSeq[VectorBase](2), length))
    ).toDF()

    Entity.insertData(entityname, data)
  }
}
