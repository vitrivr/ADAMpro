package org.vitrivr.adampro.index

import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.index.Index.IndexName
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.utils.Logging
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

case class ParameterInfo(name : String, description : String, suggestedValues : Seq[String]){}