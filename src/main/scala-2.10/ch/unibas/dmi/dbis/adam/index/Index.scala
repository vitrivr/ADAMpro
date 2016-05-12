package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.storage.partitions.PartitionHandler.PartitionID
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait Index extends Serializable {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  def indexname: IndexName

  def indextypename: IndexTypeName

  def entityname: EntityName

  /**
    * Gets path of the index.
    *
    * @return
    */
  def path : String = CatalogOperator.getIndexPath(indexname)


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
  private[index] var df: DataFrame

  /**
    * Counts the number of elements in the index.
    *
    * @return
    */
  def count = df.count()

  /**
    *
    * @return
    */
  def isStale = !CatalogOperator.isIndexUptodate(indexname)

  /**
    * Gets the metadata attached to the index.
    *
    * @return
    */
  private[index] def metadata: Serializable

  /**
    *
    * @return
    */
  def column = CatalogOperator.getIndexColumn(indexname)

  /**
    * Returns the weight set to the index. The index weight is used at query time to choose which index is used.
    */
  def weight = CatalogOperator.getIndexWeight(indexname)

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
    *
    * @return
    */
  lazy val pk = CatalogOperator.getEntityPK(entityname)

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
  def scan(q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int, filter: Option[DataFrame], partitions: Option[Set[PartitionID]], queryID: Option[String] = None)(implicit ac: AdamContext): Set[Result] = {
    log.debug("started scanning index")

    if(isStale){
      log.warn("index is stale but still used, please re-create " + indexname)
    }

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(queryID.getOrElse(""), indextypename.name, true)

    var data = df

    //apply pre-filter
    if (filter.isDefined) {
      data = data.join(filter.get.select(pk.name), pk.name)
    }

    //TODO: possibly join on other sources and keep all data

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

  /**
    * Copies the index structure. Note that this is a volatile operation and no data is stored on disk. Note also
    * that it only returns a shallow copy.
    *
    * @param newName possibly new name for index
    * @return
    */
  private[index] def copy(newName : Option[IndexName] = None) : Index = {
    val current = this

    val index = new Index {
      def indexname: IndexName = newName.getOrElse(current.indexname)
      def indextypename: IndexTypeName = current.indextypename
      def entityname: EntityName = current.entityname
      def confidence: Float = current.confidence
      def lossy: Boolean = current.lossy
      override def isStale = current.isStale
      private[index] def metadata: Serializable = current.metadata
      def isQueryConform(nnq: NearestNeighbourQuery): Boolean = current.isQueryConform(nnq)
      protected def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int) = current.scan(data, q, distance, options, k)

      private[index] var df: DataFrame = current.df
    }

    index
  }
}

object Index {
  type IndexName = String
  type IndexTypeName = IndexTypes.IndexType
}


