package org.vitrivr.adampro.index

import org.vitrivr.adampro.catalog.CatalogOperator
import org.vitrivr.adampro.config.{AdamConfig, FieldNames}
import org.vitrivr.adampro.datatypes.FieldTypes.FEATURETYPE
import org.vitrivr.adampro.datatypes.feature.Feature._
import org.vitrivr.adampro.datatypes.feature.FeatureVectorWrapper
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.exception.{EntityNotExistingException, IndexNotExistingException, IndexNotProperlyDefinedException}
import org.vitrivr.adampro.helpers.partition.Partitioning.PartitionID
import org.vitrivr.adampro.helpers.partition._
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
//TODO: make indexes singleton? lock on index?
abstract class Index(val indexname: IndexName)(@transient implicit val ac: AdamContext) extends Serializable with Logging {

  /**
    *
    */
  lazy val pk = CatalogOperator.getPrimaryKey(entityname).get

  /**
    * Gets the entity corresponding to the given index.
    */
  lazy val entity = Entity.load(entityname)

  /**
    * Gets the entityname corresponding to the given index.
    */
  lazy val entityname = CatalogOperator.getEntityName(indexname).get

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
    * Denotes the type of the index.
    */
  def indextypename: IndexTypeName


  /**
    *
    */
  private var _data: Option[DataFrame] = None

  /**
    *
    */
  def getData(): Option[DataFrame] = {
    //cache data
    if (_data.isEmpty) {
      _data = Index.storage.get.read(indexname, Seq()).map(Some(_)).getOrElse(None)

      if (_data.isDefined) {
        _data = Some(_data.get.cache())
      }
    }

    _data
  }

  /**
    *
    * @param df
    */
  private[index] def setData(df: DataFrame): Unit = {
    _data = Some(df)
  }

  /**
    * Caches the data.
    */
  def cache(): Unit = {
    if (_data.isEmpty) {
      getData()
    }

    if (_data.isDefined) {
      _data = Some(_data.get.cache())
    }
  }

  /**
    * Gets the metadata attached to the index.
    */
  private[index] def metadata: Try[Serializable] = CatalogOperator.getIndexMeta(indexname).map(_.asInstanceOf[Serializable])

  /**
    * Counts the number of elements in the index.
    *
    * @return
    */
  def count: Long = getData().map(_.count()).getOrElse(-1)

  /**
    * Marks the data stale (e.g., if new data has been inserted to entity).
    */
  def markStale(): Unit = {
    CatalogOperator.makeIndexStale(indexname)
  }

  /**
    * Is the index data stale.
    */
  def isStale = !CatalogOperator.isIndexUptodate(indexname).get

  /**
    *
    */
  def drop(): Unit = {
    try {
      Index.storage.get.drop(indexname)
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

    var df = getData().get

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
      val rdd = getData().get.rdd.mapPartitionsWithIndex((idx, iter) => if (partitions.get.contains(idx)) iter else Iterator(), preservesPartitioning = true)
      df = ac.sqlContext.createDataFrame(rdd, df.schema)
    } else {
      if (options.get("skipPart").isDefined) {
        val partitioner = CatalogOperator.getPartitioner(indexname).get
        val toKeep = partitioner.getPartitions(q, options.get("skipPart").get.toDouble, indexname)
        log.debug("Keeping Partitions: " + toKeep.mkString(", "))
        val rdd = getData().get.rdd.mapPartitionsWithIndex((idx, iter) => if (toKeep.find(_ == idx).isDefined) iter else Iterator(), preservesPartitioning = true)
        df = ac.sqlContext.createDataFrame(rdd, df.schema)
      }
    }

    var results = scan(df, q, distance, options, k)

    val t2 = System.currentTimeMillis

    log.trace(indexname + " returning tuples in " + (t2 - t1) + " msecs")

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
    *
    * @param options
    */
  def propertiesMap(options: Map[String, String] = Map()): Map[String, String] = {
    val lb = ListBuffer[(String, String)]()

    lb.append("indexname" -> indexname)
    lb.append("entityname" -> entityname)
    lb.append("confidence" -> confidence.toString)
    lb.append("attribute" -> attribute)
    lb.append("stale" -> isStale.toString)
    lb.append("partitions" -> getData().get.rdd.getNumPartitions.toString)

    //TODO: possibly add flag for displaying or not
    val partitionInfo = getData().get.rdd.mapPartitionsWithIndex((idx, f) => {
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
  private[index] def shallowCopy(newName: Option[IndexName] = None): Index = {
    val current = this

    val index = new Index(newName.getOrElse(current.indexname))(current.ac) {
      override lazy val entityname = current.entityname
      override lazy val pk = current.pk
      override lazy val entity = current.entity
      override lazy val attribute = current.attribute

      def confidence: Float = current.confidence

      def lossy: Boolean = current.lossy

      def indextypename: IndexTypeName = current.indextypename

      def isQueryConform(nnq: NearestNeighbourQuery): Boolean = current.isQueryConform(nnq)

      override def markStale(): Unit = {}

      override def isStale = current.isStale

      protected def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, String], k: Int): DataFrame = current.scan(data, q, distance, options, k)
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

  val storage = SparkStartup.mainContext.storageHandlerRegistry.value.get("parquetindex")
  assert(storage.isDefined)

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
    * @param entity
    * @param attribute
    * @param indextypename
    * @param distance
    * @param properties
    * @param ac
    * @return
    */
  def createIndex(entity: Entity, attribute: String, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map())(implicit ac: AdamContext): Try[Index] = {
    try {
      val indexGenerator = indextypename.indexGeneratorFactoryClass.newInstance().getIndexGenerator(distance, properties)
      createIndex(entity, attribute, indexGenerator)
    } catch {
      case e: Exception => {
        Failure(e)
      }
    }
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
      if (count < indexgenerator.MINIMUM_NUMBER_OF_TUPLE) {
        return Failure(new IndexNotProperlyDefinedException("not enough tuples for creating index, needs at least " + indexgenerator.MINIMUM_NUMBER_OF_TUPLE + " but has only " + count))
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

      val generatorRes = indexgenerator.index(indexname, entity.entityname, rdd)
      val data = generatorRes._1
        .withColumnRenamed("id", entity.pk.name)
        .withColumnRenamed("value", FieldNames.featureIndexColumnName)
        .repartition(AdamConfig.defaultNumberOfPartitionsIndex)
      val meta = generatorRes._2

      CatalogOperator.createIndex(indexname, entity.entityname, attribute, indexgenerator.indextypename, meta)
      storage.get.create(indexname, Seq()) //TODO: possibly switch index to be an entity with specific fields?
      val status = storage.get.write(indexname, data, Seq(), SaveMode.ErrorIfExists, Map("allowRepartitioning" -> "false"))
      CatalogOperator.createPartitioner(indexname, AdamConfig.defaultNumberOfPartitions, null, SparkPartitioner) //TODO Currently allowRepartitioning is set to true above so we use default no of partitions


      if (status.isFailure) {
        throw status.failed.get
      }

      Index.load(indexname, false)
    } catch {
      case e: Exception => {
        CatalogOperator.dropIndex(indexname, true)
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
    val index = ac.indexLRUCache.value.get(indexname)

    if (cache) {
      index.map(_.cache())
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

    try {
      val indextypename = CatalogOperator.getIndexTypeName(indexname).get

      val constructor = indextypename.indexClass.getConstructors()(0)
      val index = constructor.newInstance(Array(indexname, ac): _*).asInstanceOf[Index]

      Success(index)
    } catch {
      case e: Exception =>
        log.error("error while loading index " + indexname.toString, e)
        Failure(e)
    }
  }

  /**
    * Drops an index.
    *
    * @param indexname name of index
    * @return true if index was dropped
    */
  def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
    //TODO: tries to load index to drop; but what if index creation went wrong? -> cannot load index
    try {
      if (!exists(indexname)) {
        return Failure(EntityNotExistingException())
      }

      Index.load(indexname).get.drop()
      ac.indexLRUCache.value.invalidate(indexname)

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