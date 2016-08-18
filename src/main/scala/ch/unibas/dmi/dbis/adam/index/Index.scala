package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FEATURETYPE
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.{EntityNotExistingException, GeneralAdamException, IndexNotExistingException, IndexNotProperlyDefinedException}
import ch.unibas.dmi.dbis.adam.helpers.partition.Partitioning.PartitionID
import ch.unibas.dmi.dbis.adam.helpers.partition.{PartitionMode, PartitionerChoice, RandomPartitioner, SparkPartitioner}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, AdamContext}
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.engine.instances.ParquetEngine
import ch.unibas.dmi.dbis.adam.storage.handler.IndexFlatFileHandler
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, DataFrame}
import org.scalatest.path

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
//TODO: make indexes singleton? lock on index?
//TODO: adjust architecture, move logic of loading data to here
abstract class Index(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  val indexname: IndexName
  val indextypename: IndexTypeName

  /**
    *
    * @return
    */
  lazy val pk = CatalogOperator.getPrimaryKey(entityname).get

  /**
    * Gets the entity corresponding to the given index.
    */
  lazy val entity = Entity.load(entityname)

  /**
    * Gets the entityname corresponding to the given index.
    */
  val entityname: EntityName

  /**
    * Gets the indexed attribute.
    */
  lazy val attribute = CatalogOperator.getIndexAttribute(indexname).get

  /**
    * Confidence towards the index. Confidence of 1 means very confident in index results (i.e., precise results).
    */
  def confidence: Float

  /**
    * Score. Multiplier in index choice.
    */
  def score: Float = confidence

  /**
    * Denotes whether the index leads to false negatives, i.e., elements are dropped although they shouldn't be.
    */
  def lossy: Boolean

  /**
    *
    */
  lazy val storage = SparkStartup.indexStorageHandler

  /**
    *
    */
  private[index] var data: DataFrame

  /**
    * Gets the metadata attached to the index.
    */
  private[index] def metadata: Serializable

  /**
    * Counts the number of elements in the index.
    *
    * @return
    */
  def count = data.count()

  /**
    * Marks the data stale (e.g., if new data has been inserted to entity).
    */
  def markStale(): Unit = {
    CatalogOperator.makeIndexStale(indexname)
  }

  /**
    * Is the index data stale.
    */
  def isStale = !isUptodate

  /**
    * Is the index up to date.
    */
  def isUptodate = CatalogOperator.isIndexUptodate(indexname).get

  /**
    *
    */
  def drop(): Unit = {
    try {
      storage.drop(indexname)
    } catch {
      case e: Exception =>
        log.error("exception when dropping index " + indexname, e)
    } finally {
      CatalogOperator.dropIndex(indexname)
    }
  }

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
    * Scans the index.
    *
    * @param nnq    query for scanning the index
    * @param filter pre-filter
    * @return
    */
  def scan(nnq: NearestNeighbourQuery, filter: Option[DataFrame])(implicit ac: AdamContext): DataFrame = {
    log.debug("scan index")
    scan(nnq.q, nnq.distance, nnq.options, nnq.k, filter, nnq.partitions, nnq.queryID)
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
  def scan(q: FeatureVector, distance: DistanceFunction, options: Map[String, String] = Map(), k: Int, filter: Option[DataFrame], partitions: Option[Set[PartitionID]], queryID: Option[String] = None)(implicit ac: AdamContext): DataFrame = {
    val t1 = System.currentTimeMillis
    log.debug("started scanning index")

    if (isStale) {
      log.warn("index is stale but still used, please re-create " + indexname)
    }

    var df = data

    //apply pre-filter
    var ids = mutable.Set[Any]()

    if (filter.isDefined) {
      ids ++= filter.get.select(pk.name).collect().map(_.getAs[Any](pk.name))
    }

    if (ids.nonEmpty) {
      val idsbc = ac.sc.broadcast(ids)
      df = ac.sqlContext.createDataFrame(df.rdd.filter(x => idsbc.value.contains(x.getAs[Any](pk.name))), df.schema)
    }

    //choose specific partition
    if (partitions.isDefined) {
      val rdd = data.rdd.mapPartitionsWithIndex((idx, iter) => if (partitions.get.contains(idx)) iter else Iterator(), preservesPartitioning = true)
      df = ac.sqlContext.createDataFrame(rdd, df.schema)
    }

    var results = scan(df, q, distance, options, k)

    val t2 = System.currentTimeMillis

    log.debug(indexname + " returning tuples in " + (t2 - t1) + " msecs")

    results
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
  protected def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame


  /**
    * Returns stored index options.
    */
  def options = CatalogOperator.getIndexOption(indexname)


  /**
    * Returns a map of properties to the index. Useful for printing.
    */
  def propertiesMap: Map[String, String] = {
    val lb = ListBuffer[(String, String)]()

    lb.append("indexname" -> indexname)
    lb.append("entityname" -> entityname)
    lb.append("confidence" -> confidence.toString)
    lb.append("attribute" -> attribute)
    lb.append("stale" -> isStale.toString)
    lb.append("partitions" -> data.rdd.getNumPartitions.toString)

    //TODO: possibly add flag for displaying or not
    val partitionInfo = data.rdd.mapPartitionsWithIndex((idx, f) => {
      Iterator((idx, f.length))
    })
    lb.append("tuplesPerPartition" -> partitionInfo.collect().map { case (id, length) => "(" + id + "," + length + ")" }.mkString)

    lb.toMap
  }

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

      protected def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int) = current.scan(data, q, distance, options, k)

      private[index] var data: DataFrame = current.data
    }

    index
  }

  override def equals(that: Any): Boolean =
    that match {
      case that: Index =>
        this.indexname.equals(that.indexname)
      case _ => false
    }

  override def hashCode: Int = indexname.hashCode
}

object Index extends Logging {
  type IndexName = EntityName
  type IndexTypeName = IndexTypes.IndexType

  val storage = SparkStartup.indexStorageHandler

  /**
    * Creates an index that is unique and which follows the naming rules of indexes.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute that is being indexed
    * @param indextype  type of index
    * @return
    */
  private[index] def createIndexName(entityname: EntityName, attribute: String, indextype: IndexTypeName): String = {
    val indexes = CatalogOperator.listIndexes(Some(entityname), Some(indextype)).get

    var indexname = ""

    var i = indexes.length
    do {
      indexname = entityname + "_" + attribute + "_" + indextype.name + "_" + i
      i += 1
    } while (indexes.contains(indexname))

    indexname
  }


  /**
    * Creates an index. Performs preparatory tasks and checks.
    *
    * @param entity         entity
    * @param attribute      the attribute to index
    * @param indexgenerator generator to create index
    * @return index
    */
  def createIndex(entity: Entity, attribute: String, indexgenerator: IndexGenerator)(implicit ac: AdamContext): Try[Index] = {
    try {
      if (!entity.schema().map(_.name).contains(attribute)) {
        return Failure(new IndexNotProperlyDefinedException("attribute not existing in entity " + entity.entityname + entity.schema().map(_.name).mkString("(", ",", ")")))
      }

      val columnFieldtype = entity.schema().filter(_.name == attribute).map(_.fieldtype).head
      if (columnFieldtype != FEATURETYPE) {
        return Failure(new IndexNotProperlyDefinedException(attribute + " is of type " + columnFieldtype.name + ", not feature"))
      }

      val count = entity.count
      if (count < IndexGenerator.MINIMUM_NUMBER_OF_TUPLE) {
        return Failure(new IndexNotProperlyDefinedException("not enough tuples for creating index, needs at least " + IndexGenerator.MINIMUM_NUMBER_OF_TUPLE + " but has only " + count))
      }

      val indexname = createIndexName(entity.entityname, attribute, indexgenerator.indextypename)

      createIndex(indexname, entity, attribute, indexgenerator)
    } catch {
      case e: Exception => {
        Failure(e)
      }
    }
  }

  /**
    * Performs the creation of the index.
    *
    * @param indexname      name of the index
    * @param entity         entity
    * @param attribute      the attribute to index
    * @param indexgenerator generator to create index
    * @return index
    */
  private def createIndex(indexname: String, entity: Entity, attribute: String, indexgenerator: IndexGenerator)(implicit ac: AdamContext): Try[Index] = {
    try {
      val rdd: RDD[IndexingTaskTuple[_]] = entity.getAttributeData(attribute).get.map { x => IndexingTaskTuple(x.getAs[Any](entity.pk.name), x.getAs[FeatureVectorWrapper](attribute).vector) }

      val index = indexgenerator.index(indexname, entity.entityname, rdd)
      index.data = index
        .data
        .withColumnRenamed("id", entity.pk.name)
        .withColumnRenamed("value", FieldNames.featureIndexColumnName)

      CatalogOperator.createIndex(indexname, entity.entityname, attribute, indexgenerator.indextypename, index.metadata)
      storage.create(indexname, Seq()) //TODO: possibly switch index to be an entity with specific fields?
      val status = storage.write(indexname, index.data, SaveMode.ErrorIfExists, Map("allowRepartitioning" -> "true"))

      if (status.isFailure) {
        throw status.failed.get
      }

      Index.load(indexname, false)
    } catch {
      case e: Exception => {
        CatalogOperator.dropIndex(indexname)
        Failure(e)
      }
    }
  }

  /**
    * Checks whether index exists.
    *
    * @param indexname name of index
    * @return
    */
  def exists(indexname: IndexName)(implicit ac: AdamContext): Boolean = CatalogOperator.existsIndex(indexname).get

  /**
    * Checks whether index exists.
    *
    * @param entityname    name of entity
    * @param attribute     name of attribute
    * @param indextypename index type to use for indexing
    * @return
    */
  def exists(entityname: EntityName, attribute: String, indextypename: IndexTypeName)(implicit ac: AdamContext): Boolean = CatalogOperator.existsIndex(entityname, attribute, indextypename).get

  /**
    * Lists indexes.
    *
    * @param entityname    name of entity
    * @param indextypename name of index type
    * @return
    */
  def list(entityname: Option[EntityName] = None, indextypename: Option[IndexTypeName] = None)(implicit ac: AdamContext): Seq[Try[Index]] = {
    CatalogOperator.listIndexes(entityname, indextypename).get.map(load(_))
  }

  /**
    * Loads index into cache.
    *
    * @param indexname name of index
    * @return
    */
  def load(indexname: IndexName, cache: Boolean = false)(implicit ac: AdamContext): Try[Index] = {
    try {

      if (!IndexLRUCache.contains(indexname) && !exists(indexname)) {
        return Failure(new IndexNotExistingException())
      }

      val index = IndexLRUCache.get(indexname)

      if (cache) {
        index.get.data.rdd.setName(indexname).cache()
      }

      index
    } catch {
      case e: Exception => Failure(e)
    }
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

    try {
      val df = storage.read(indexname)
      if (df.isFailure) {
        return Failure(df.failed.get)
      }

      val entityname = CatalogOperator.getEntityName(indexname).get
      val meta = CatalogOperator.getIndexMeta(indexname)

      if (meta.isFailure) {
        return Failure(meta.failed.get)
      }

      val indextypename = CatalogOperator.getIndexTypeName(indexname).get

      val index = indextypename.index(indexname, entityname, df.get, meta.get, ac)

      Success(index)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Drops an index.
    *
    * @param indexname name of index
    * @return true if index was dropped
    */
  def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
    try {
      if (!exists(indexname)) {
        return Failure(EntityNotExistingException())
      }

      Index.load(indexname).get.drop()
      IndexLRUCache.invalidate(indexname)

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Drops all indexes for a given entity.
    *
    * @param entityname name of entity
    * @return
    */
  def dropAll(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
    val indexes = CatalogOperator.listIndexes(Some(entityname)).get

    indexes.foreach {
      indexname => drop(indexname)
    }

    Success(null)
  }
}