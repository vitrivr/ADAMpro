package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.partitions.PartitionHandler.PartitionID
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
abstract class Index(private[index] var dataframe : DataFrame) {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def indexname: IndexName

  def indextypename: IndexTypeName

  def entityname: EntityName


  /**
    * Confidence towards the index. Confidence of 1 means very confident in index results (i.e., precise results).
    */
  def confidence: Float

  /**
    * Denotes whether the index leads to false negatives, i.e., elements are dropped although they shouldn't be.
    */
  def lossy: Boolean

  /**
    *
    */
  private[index] def df: DataFrame = dataframe

  /**
    * Counts the number of elements in the index.
    *
    * @return
    */
  def count = df.count()

  /**
    * Gets the metadata attached to the index.
    *
    * @return
    */
  private[index] def metadata: Serializable

  /**
    * Returns whether the index can be used with the given query.
    * (Note that the returned value is only a recommendation, and the index can be 'forced' to be used with the given
    * distance function, etc.)
    *
    * @param nnq
    * @return true if index can be used for given query, false if not
    */
  def isQueryConform(nnq: NearestNeighbourQuery): Boolean

  /**
    * Scans the index.
    *
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @param filter   optional pre-filter for Boolean query
    * @param queryID  optional query id
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  def scan(q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int, filter: Option[Set[TupleID]], partitions: Option[Set[PartitionID]], queryID: Option[String] = None)(implicit ac: AdamContext): Set[Result] = {
    log.debug("started scanning index")

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(queryID.getOrElse(""), indextypename.name, true)

    var data = df

    //apply pre-filter
    if (filter.isDefined) {
      data = data.filter(df(FieldNames.idColumnName) isin (filter.get.toSeq: _*))
    }

    //choose specific partition
    if (partitions.isDefined) {
      val rdd = data.rdd.mapPartitionsWithIndex((idx, iter) => if (partitions.get.contains(idx)) iter else Iterator(), true)
      data = ac.sqlContext.createDataFrame(rdd, df.schema)
    }

    scan(data, q, distance, options, k)
  }

  /**
    * Scans the index.
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  protected def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int): Set[Result]
}

object Index {
  type IndexName = String
  type IndexTypeName = IndexTypes.IndexType
}


