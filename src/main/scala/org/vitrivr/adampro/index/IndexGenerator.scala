package org.vitrivr.adampro.index

import org.apache.spark.sql.{Row, DataFrame}
import org.apache.spark.util.random.Sampling
import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.datatypes.TupleID._
import org.vitrivr.adampro.datatypes.vector.Vector.DenseSparkVector
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.datatypes.vector.Vector

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
    * Indexes the data.
    *
    * @param data      raw data to index
    * @param attribute name of attribute to index
    * @return
    */
  def index(data: DataFrame, attribute: String): (DataFrame, Serializable)

  /**
    *
    * @param data
    * @param minN
    * @param distinct
    * @return
    */
  private def getSample(data: DataFrame, minN: Int, distinct: Boolean = false): Seq[Row] = {
    val n = data.count()
    val fraction = Sampling.computeFractionForSampleSize(minN, n, withReplacement = false)

    var sample = data.sample(false, fraction).collect()
    if (sample.length < minN) sample = sample ++ data.take(minN)

    if (distinct) sample = sample.distinct

    if (sample.length < minN) {
      log.warn("not enough distinct data found when indexing, possibly retry")
    }

    sample
  }

  /**
    *
    * @param minN
    * @return
    */
  protected def getSample(minN: Int): (DataFrame) => Seq[Row] = (data: DataFrame) => getSample(data, minN)


  /**
    *
    * @param minN
    * @return
    */
  protected def getSample(minN: Int, attribute : String): (DataFrame) => Seq[IndexingTaskTuple] =
    (data: DataFrame) => {
    getSample(data, minN).map(r => IndexingTaskTuple(r.getAs[TupleID](FieldNames.internalIdColumnName), Vector.conv_dspark2vec(r.getAs[DenseSparkVector](attribute))))
  }

}


trait IndexGeneratorFactory extends Serializable with Logging {
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator

  def parametersInfo: Seq[ParameterInfo]
}

case class ParameterInfo(name: String, description: String, suggestedValues: Seq[String]) {}