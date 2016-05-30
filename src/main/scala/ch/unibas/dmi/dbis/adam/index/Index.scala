package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FEATURETYPE
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.{IndexNotProperlyDefinedException, GeneralAdamException, IndexNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName, PartitionID}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.storage.partition.PartitionMode
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.{Failure, Random, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
abstract class Index(implicit ac: AdamContext) extends Serializable with Logging {

  val indexname: IndexName

  val indextypename: IndexTypeName

  lazy val entity = Entity.load(entityname)

  val entityname: EntityName

  /**
    * Gets path of the index.
    *
    * @return
    */
  private[index] def path: String = CatalogOperator.getIndexPath(indexname)


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
  private[index] var data: DataFrame

  /**
    * Counts the number of elements in the index.
    *
    * @return
    */
  def count = data.count()

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
    * @param nnq nearest neighbour query object
    * @return true if index can be used for given query, false if not
    */
  def isQueryConform(nnq: NearestNeighbourQuery): Boolean

  /**
    *
    * @param weight new weights to set to index
    * @return
    */
  def setWeight(weight: Float): Boolean = {
    CatalogOperator.updateIndexWeight(indexname, weight)
  }

  /**
    *
    * @return
    */
  lazy val pk = CatalogOperator.getEntityPK(entityname)


  /**
    * Scans the index.
    *
    * @param nnq    query for scanning the index
    * @param filter pre-filter
    * @return
    */
  def scan(nnq: NearestNeighbourQuery, filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    log.debug("scan index")
    val results = scan(nnq.q, nnq.distance, nnq.options, nnq.k, filter, nnq.partitions, nnq.queryID)
    val rdd = ac.sc.parallelize(results.map(result => Row(result.distance, result.tid)).toSeq)
    val fields = Result.resultSchema(pk.name)
    ac.sqlContext.createDataFrame(rdd, fields)
  }


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
  def scan(q: FeatureVector, distance: DistanceFunction, options: Map[String, String] = Map(), k: Int, filter: Option[DataFrame], partitions: Option[Set[PartitionID]], queryID: Option[String] = None)(implicit ac: AdamContext): Set[Result] = {
    log.debug("started scanning index")

    if (isStale) {
      log.warn("index is stale but still used, please re-create " + indexname)
    }

    ac.sc.setLocalProperty("spark.scheduler.pool", "index")
    ac.sc.setJobGroup(queryID.getOrElse(""), indextypename.name, interruptOnCancel = true)

    var df = data

    //apply pre-filter
    if (filter.isDefined) {
      df = data.join(filter.get.select(pk.name), pk.name)
    }

    //TODO: possibly join on other sources and keep all data

    //choose specific partition
    if (partitions.isDefined) {
      val rdd = data.rdd.mapPartitionsWithIndex((idx, iter) => if (partitions.get.contains(idx)) iter else Iterator(), preservesPartitioning = true)
      df = ac.sqlContext.createDataFrame(rdd, df.schema)
    }

    scan(df, q, distance, options, k)
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
  private[index] def copy(newName: Option[IndexName] = None): Index = {
    val current = this

    val index = new Index {
      val indexname: IndexName = newName.getOrElse(current.indexname)
      val indextypename: IndexTypeName = current.indextypename
      lazy val entityname: EntityName = current.entityname

      def confidence: Float = current.confidence

      def lossy: Boolean = current.lossy

      override def isStale = current.isStale

      private[index] def metadata: Serializable = current.metadata

      def isQueryConform(nnq: NearestNeighbourQuery): Boolean = current.isQueryConform(nnq)

      protected def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int) = current.scan(data, q, distance, options, k)

      private[index] var data: DataFrame = current.data
    }

    index
  }
}

object Index extends Logging {
  type IndexName = String
  type IndexTypeName = IndexTypes.IndexType

  private val storage = SparkStartup.indexStorage

  private val lock = new Object() //TODO: make indexes singleton? lock on entity?

  type PartitionID = Int


  /**
    * Creates an index that is unique and which follows the naming rules of indexes.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute that is being indexed
    * @param indextype  type of index
    * @return
    */
  private def createIndexName(entityname: EntityName, attribute: String, indextype: IndexTypeName): String = {
    val indexes = CatalogOperator.listIndexes(entityname, indextype).map(_._1)

    var indexname = ""

    var i = indexes.length
    do {
      indexname = entityname + "_" + attribute + "_" + indextype.name + "_" + i
      i += 1
    } while (indexes.contains(indexname))

    indexname
  }


  /**
    * Creates an index.
    *
    * @param entity         entity
    * @param attribute      the attribute to index
    * @param indexgenerator generator to create index
    * @return index
    */
  def createIndex(entity: Entity, attribute: String, indexgenerator: IndexGenerator)(implicit ac: AdamContext): Try[Index] = {
    lock.synchronized {
      if (!entity.schema.map(_.name).contains(attribute)) {
        return Failure(new IndexNotProperlyDefinedException("attribute not existing in entity " + entity.entityname + entity.schema.map(_.name).mkString("(", ",", ")")))
      }

      val columnFieldtype = entity.schema.filter(_.name == attribute).map(_.fieldtype).head
      if (columnFieldtype != FEATURETYPE) {
        return Failure(new IndexNotProperlyDefinedException(attribute + " is of type " + columnFieldtype.name + ", not feature"))
      }

      val count = entity.count
      if (count < IndexGenerator.MINIMUM_NUMBER_OF_TUPLE) {
        return Failure(new IndexNotProperlyDefinedException("not enough tuples for creating index, needs at least " + IndexGenerator.MINIMUM_NUMBER_OF_TUPLE + " but has only " + count))
      }

      val indexname = createIndexName(entity.entityname, attribute, indexgenerator.indextypename)
      val rdd: RDD[IndexingTaskTuple[_]] = entity.getIndexableFeature(attribute).map { x => IndexingTaskTuple(x.getAs[Any](entity.pk.name), x.getAs[FeatureVectorWrapper](attribute).vector) }

      val index = indexgenerator.index(indexname, entity.entityname, rdd)
      index.data = index
        .data
        .repartition(AdamConfig.defaultNumberOfPartitions)
        .withColumnRenamed("id", entity.pk.name)
        .withColumnRenamed("value", FieldNames.featureIndexColumnName)
      val path = storage.write(indexname, index.data, None, allowRepartitioning = true)
      if (path.isSuccess) {
        CatalogOperator.createIndex(indexname, path.get, entity.entityname, attribute, indexgenerator.indextypename, index.metadata)
        Success(index)
      } else {
        Failure(path.failed.get)
      }
    }
  }

  /**
    * Checks whether index exists.
    *
    * @param indexname name of index
    * @return
    */
  def exists(indexname: IndexName)(implicit ac: AdamContext): Boolean = CatalogOperator.existsIndex(indexname)

  /**
    * Lists indexes.
    *
    * @param entityname name of entity
    * @param indextypename name of index type
    * @return
    */
  def list(entityname: EntityName = null, indextypename: IndexTypeName = null)(implicit ac: AdamContext): Seq[Try[Index]] = {
    CatalogOperator.listIndexes(entityname, indextypename).map(index => load(index._1))
  }

  /**
    * Loads index into cache.
    *
    * @param indexname name of index
    * @return
    */
  def load(indexname: IndexName, cache: Boolean = false)(implicit ac: AdamContext): Try[Index] = {
    if (!IndexLRUCache.contains(indexname) && !exists(indexname)) {
      return Failure(new IndexNotExistingException())
    }

    val index = IndexLRUCache.get(indexname)

    if (cache) {
      index.get.data.rdd.setName(indexname).cache()
    }

    index
  }

  /**
    * Loads the index metadata without loading the data itself yet.
    *
    * @param indexname name of index
    * @return
    */
  private[index] def loadIndexMetaData(indexname: IndexName)(implicit ac: AdamContext): Try[Index] = {
    if (!exists(indexname)) {
      Failure(IndexNotExistingException())
    }

    val path = CatalogOperator.getIndexPath(indexname)
    val df = storage.read(path)
    if (df.isFailure) {
      return Failure(df.failed.get)
    }

    val entityname = CatalogOperator.getEntitynameFromIndex(indexname)
    val meta = CatalogOperator.getIndexMeta(indexname)

    val indextypename = CatalogOperator.getIndexTypeName(indexname)

    val index = indextypename.index(indexname, entityname, df.get, meta)

    Success(index)
  }

  /**
    * Drops an index.
    *
    * @param indexname name of index
    * @return true if index was dropped
    */
  def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
    lock.synchronized {
      storage.drop(CatalogOperator.getIndexPath(indexname))
      CatalogOperator.dropIndex(indexname)
      Success(null)
    }
  }

  /**
    * Drops all indexes for a given entity.
    *
    * @param entityname name of entity
    * @return
    */
  def dropAll(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
    val indexes = CatalogOperator.listIndexes(entityname).map(_._1)

    indexes.foreach {
      indexname => storage.drop(CatalogOperator.getIndexPath(indexname))
    }

    CatalogOperator.dropAllIndexes(entityname)

    Success(null)
  }


  /**
    * Repartition index.
    *
    * @param index index
    * @param nPartitions number of partitions
    * @param join other dataframes to join on, on which the partitioning is performed
    * @param cols columns to partition on, if not specified the primary key is used
    * @param mode partition mode
    * @return
    */
  def repartition(index: Index, nPartitions: Int, join: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Index] = {
    var data = index.data.join(index.entity.get.data.get, index.pk.name)

    //TODO: possibly add own partitioner
    //data.map(r => (r.getAs(cols.get.head), r)).partitionBy(new HashPartitioner())

    //TODO: possibly consider replication
    //http://stackoverflow.com/questions/31624622/is-there-a-way-to-change-the-replication-factor-of-rdds-in-spark
    //data.persist(StorageLevel.MEMORY_ONLY_2) new StorageLevel(...., N)

    if (join.isDefined) {
      data = data.join(join.get, index.pk.name)
    }

    data = if (cols.isDefined) {
      val entityColNames = data.schema.map(_.name)
      if (!cols.get.forall(name => entityColNames.contains(name))) {
        Failure(throw new GeneralAdamException("one of the columns " + cols.mkString(",") + " is not existing in entity " + index.entityname + entityColNames.mkString("(", ",", ")")))
      }

      data.repartition(nPartitions, cols.get.map(col): _*)
    } else {
      data.repartition(nPartitions, data(index.pk.name))
    }

    data = data.select(index.pk.name, FieldNames.featureIndexColumnName)

    mode match {
      case PartitionMode.CREATE_NEW =>
        val newName = createIndexName(index.entityname, index.column, index.indextypename)
        val path = storage.write(newName, data)
        CatalogOperator.createIndex(newName, path.get, index.entityname, index.column, index.indextypename, index.metadata)
        IndexLRUCache.invalidate(newName)

        Success(load(newName).get)

      case PartitionMode.CREATE_TEMP =>
        val newName = createIndexName(index.entityname, index.column, index.indextypename)

        val newIndex = index.copy(Some(newName))
        newIndex.data = data

        IndexLRUCache.put(newName, newIndex)
        Success(newIndex)

      case PartitionMode.REPLACE_EXISTING =>
        val oldPath = index.path

        index.data = data
        var newPath = ""

        do {
          newPath = index.path + "-rep" + Random.nextInt
        } while (SparkStartup.indexStorage.exists(newPath).get)

        SparkStartup.indexStorage.write(index.indexname, data, Some(newPath))
        CatalogOperator.updateIndexPath(index.indexname, newPath)
        SparkStartup.indexStorage.drop(oldPath)

        IndexLRUCache.invalidate(index.indexname)

        Success(index)

      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }

  /**
    * Resets all weights to default weight.
    *
    * @param entityname name of entity
    */
  def resetAllWeights(entityname: EntityName): Boolean = {
    CatalogOperator.resetIndexWeight(entityname)
  }
}




