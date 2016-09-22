package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait IndexGenerator extends Serializable with Logging {
  private[index] val MINIMUM_NUMBER_OF_TUPLE = 1000

  //TODO: have index generator support pre-processing steps, e.g., PCA, etc. (but only if accepted by the index)

  /**
    *
    * @return
    */
  def indextypename: IndexTypes.IndexType

  /**
    *
    * @param indexname
    * @param entityname
    * @param data
    * @return
    */
  def index(indexname : IndexName, entityname : EntityName, data: RDD[IndexingTaskTuple[_]]):  (DataFrame, Serializable)
}


trait IndexGeneratorFactory extends Serializable with Logging {
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator

  def parametersInfo : Seq[ParameterInfo]
}

case class ParameterInfo(name : String, description : String, suggestedValues : Seq[String]){
  def this(name : String, description : String, suggestedValues : Seq[Int]){
    this(name, description, suggestedValues.map(_.toString))
  }


}