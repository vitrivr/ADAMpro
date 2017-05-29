package org.vitrivr.adampro.index

import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.AttributeTypes.VECTORTYPE
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity._
import org.vitrivr.adampro.exception.{IndexNotExistingException, IndexNotProperlyDefinedException}
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.IndexPartitioner.log
import org.vitrivr.adampro.index.partition.Partitioning.PartitionID
import org.vitrivr.adampro.index.partition._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
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
    * Gets the entityname corresponding to the given index.
    */
  lazy val entityname = SparkStartup.catalogOperator.getEntityName(indexname).get

  /**
    *
    */
  lazy val pk = SparkStartup.catalogOperator.getPrimaryKey(entityname).get

  /**
    * Gets the entity corresponding to the given index.
    */
  lazy val entity = Entity.load(entityname)

  /**
    * Gets the indexed attribute.
    */
  lazy val attribute = SparkStartup.catalogOperator.getIndexAttribute(indexname).get

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
      val data = Index.getStorage().get.read(indexname, Seq())

      if (data.isFailure) {
        log.error("error while reading index data: " + data.failed.get.getMessage, data.failed.get)
      }

      _data = data.map(Some(_)).getOrElse(None)


      //caching index data
      import scala.concurrent.ExecutionContext.Implicits.global
      val cachingFut = Future {
        if (_data.isDefined) {
          val cachedData = Some(_data.get.persist(StorageLevel.MEMORY_ONLY))
          cachedData.get.count() //counting for caching
          cachedData
        } else {
          None
        }
      }
      cachingFut.onSuccess {
        case cachedData => if (cachedData.isDefined) {
          _data = cachedData
        }
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
      _data = Some(_data.get.persist(StorageLevel.MEMORY_ONLY))
      _data.get.count() //counting for caching
    }
  }

  /**
    * Gets the metadata attached to the index.
    */
  private[index] def metadata: Try[Serializable] = SparkStartup.catalogOperator.getIndexMeta(indexname).map(_.asInstanceOf[Serializable])

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
    SparkStartup.catalogOperator.makeIndexStale(indexname)
  }

  /**
    * Is the index data stale.
    */
  def isStale = !SparkStartup.catalogOperator.isIndexUptodate(indexname).get

  /**
    *
    */
  def drop(): Unit = {
    try {
      Index.getStorage().get.drop(indexname)
    } catch {
      case e: Exception =>
        log.error("exception when dropping index " + indexname, e)
    } finally {
      SparkStartup.catalogOperator.dropIndex(indexname)
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
  def scan(nnq: NearestNeighbourQuery, filter: Option[DataFrame])(tracker: OperationTracker)(implicit ac: AdamContext): DataFrame = {
    log.debug("scan index")
    scan(nnq.q, nnq.distance, nnq.options, nnq.k, filter, nnq.partitions, nnq.queryID)(tracker)
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
  def scan(q: MathVector, distance: DistanceFunction, options: Map[String, String] = Map(), k: Int, filter: Option[DataFrame], partitions: Option[Set[PartitionID]], queryID: Option[String] = None)(tracker: OperationTracker)(implicit ac: AdamContext): DataFrame = {
    log.debug("started scanning index")

    if (isStale) {
      log.warn("index is stale but still used, please re-create " + indexname)
    }

    var df = getData().get

    //apply pre-filter
    val funnel = new Funnel[Any] {
      override def funnel(t: Any, primitiveSink: PrimitiveSink): Unit =
        t match {
          case s: String => primitiveSink.putUnencodedChars(s)
          case l: Long => primitiveSink.putLong(l)
          case i: Int => primitiveSink.putInt(i)
          case _ => primitiveSink.putUnencodedChars(t.toString)
        }
    }
    val ids = BloomFilter.create[Any](funnel, 1000, 0.05)

    if (filter.isDefined) {
      filter.get.select(pk.name).collect().map(_.getAs[Any](pk.name)).toSeq.foreach {
        ids.put(_)
      }

      val idsBc = ac.sc.broadcast(ids)
      tracker.addBroadcast(idsBc)
      val filterUdf = udf((arg: Any) => idsBc.value.mightContain(arg))

      df = df.filter(filterUdf(col(pk.name)))
    }

    //choose specific partition
    if (partitions.isDefined) {
      val rdd = df.rdd.mapPartitionsWithIndex((idx, iter) => if (partitions.get.contains(idx)) iter else Iterator(), preservesPartitioning = true)
      df = ac.sqlContext.createDataFrame(rdd, df.schema)
    } else if (options.get("skipPart").isDefined) {
      val partitioner = SparkStartup.catalogOperator.getPartitioner(indexname).get
      val toKeep = partitioner.getPartitions(q, options.get("skipPart").get.toDouble, indexname)
      log.trace("keeping partitions: " + toKeep.mkString(", "))
      val rdd = df.rdd.mapPartitionsWithIndex((idx, iter) => if (toKeep.find(_ == idx).isDefined) iter else Iterator(), preservesPartitioning = true)
      df = ac.sqlContext.createDataFrame(rdd, df.schema)
    }

    scan(df, q, distance, options, k)(tracker)
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
  protected def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker: OperationTracker): DataFrame


  /**
    * Returns stored index options.
    */
  def options = SparkStartup.catalogOperator.getIndexOption(indexname)


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

      protected def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker: OperationTracker): DataFrame = current.scan(data, q, distance, options, k)(tracker)
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

  private[index] def getStorage()(implicit ac: AdamContext) = ac.storageHandlerRegistry.value.get("parquetindex")

  /**
    * Creates an index that is unique and which follows the naming rules of indexes.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute that is being indexed
    * @param indextype  type of index
    * @return
    */
  private[index] def createIndexName(entityname: EntityName, attribute: String, indextype: IndexTypeName)(implicit ac: AdamContext): String = {
    val indexes = SparkStartup.catalogOperator.listIndexes(Some(entityname), Some(attribute), Some(indextype)).get

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
  def createIndex(entity: Entity, attribute: String, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map())(tracker: OperationTracker)(implicit ac: AdamContext): Try[Index] = {
    try {
      val indexGenerator = indextypename.indexGeneratorFactoryClass.newInstance().getIndexGenerator(distance, properties + ("n" -> entity.count.toString))
      createIndex(entity, attribute, indexGenerator)(tracker)
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
  def createIndex(entity: Entity, attribute: String, indexgenerator: IndexGenerator)(tracker: OperationTracker)(implicit ac: AdamContext): Try[Index] = {
    try {
      if (!entity.schema().map(_.name).contains(attribute)) {
        return Failure(new IndexNotProperlyDefinedException("attribute " + attribute + " not existing in entity " + entity.entityname + entity.schema(fullSchema = false).map(_.name).mkString("(", ",", ")")))
      }

      val attributetype = entity.schema().filter(_.name == attribute).map(_.attributeType).head
      if (attributetype != VECTORTYPE) {
        return Failure(new IndexNotProperlyDefinedException(attribute + " is of type " + attributetype.name + ", not feature"))
      }

      val count = entity.count
      if (count < indexgenerator.MINIMUM_NUMBER_OF_TUPLE) {
        return Failure(new IndexNotProperlyDefinedException("not enough tuples for creating index, needs at least " + indexgenerator.MINIMUM_NUMBER_OF_TUPLE + " but has only " + count))
      }

      val indexname = createIndexName(entity.entityname, attribute, indexgenerator.indextypename)

      createIndex(indexname, entity, attribute, indexgenerator)(tracker)
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
  private def createIndex(indexname: String, entity: Entity, attribute: String, indexgenerator: IndexGenerator)(tracker: OperationTracker)(implicit ac: AdamContext): Try[Index] = {
    try {
      val indexableData = entity.getAttributeData(attribute).get
        .select(AttributeNames.internalIdColumnName, attribute)

      val generatorRes = indexgenerator.index(indexableData, attribute)(tracker)
      val data = generatorRes._1
        .withColumnRenamed(AttributeNames.internalIdColumnName, entity.pk.name)
        .drop(attribute) //remove feature column from index
        .repartition(ac.config.defaultNumberOfPartitionsIndex)
      val meta = generatorRes._2

      SparkStartup.catalogOperator.createIndex(indexname, entity.entityname, attribute, indexgenerator.indextypename, meta)
      getStorage().get.create(indexname, Seq()) //TODO: possibly switch index to be an entity with specific fields?
      val status = getStorage().get.write(indexname, data, Seq(), SaveMode.ErrorIfExists, Map("allowRepartitioning" -> "false"))
      SparkStartup.catalogOperator.createPartitioner(indexname, ac.config.defaultNumberOfPartitions, null, SparkPartitioner) //TODO Currently allowRepartitioning is set to true above so we use default no of partitions


      if (status.isFailure) {
        throw status.failed.get
      }

      Index.load(indexname, false)
    } catch {
      case e: Exception => {
        SparkStartup.catalogOperator.dropIndex(indexname, true)
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
  def exists(indexname: IndexName)(implicit ac: AdamContext): Boolean = SparkStartup.catalogOperator.existsIndex(indexname).get

  /**
    * Checks whether index exists.
    *
    * @param entityname    name of entity
    * @param attribute     name of attribute
    * @param indextypename index type to use for indexing
    * @return
    */
  def exists(entityname: EntityName, attribute: String, indextypename: IndexTypeName, acceptStale: Boolean)(implicit ac: AdamContext): Boolean = {
    SparkStartup.catalogOperator.existsIndex(entityname, attribute, indextypename, acceptStale).get
  }

  /**
    * Lists indexes.
    *
    * @param entityname    name of entity
    * @param attribute     name of attribute
    * @param indextypename name of index type
    * @return
    */
  def list(entityname: Option[EntityName] = None, attribute: Option[String] = None, indextypename: Option[IndexTypeName] = None)(implicit ac: AdamContext): Seq[Try[Index]] = {
    SparkStartup.catalogOperator.listIndexes(entityname, attribute, indextypename).get.map(load(_))
  }

  /**
    * Loads index into cache.
    *
    * @param indexname name of index
    * @return
    */
  def load(indexname: IndexName, cache: Boolean = false)(implicit ac: AdamContext): Try[Index] = {
    val index = ac.indexLRUCache.get(indexname)

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
      Failure(IndexNotExistingException.withIndexname(indexname))
    }

    try {
      val indextypename = SparkStartup.catalogOperator.getIndexTypeName(indexname).get

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
        return Failure(IndexNotExistingException.withIndexname(indexname))
      }

      try {
        Index.load(indexname).get.drop()
        ac.indexLRUCache.invalidate(indexname)
      } catch {
        case e: Exception =>
          //possibly if index could not be loaded, we should still be able to drop the index
          Index.getStorage().get.drop(indexname)
          SparkStartup.catalogOperator.dropIndex(indexname)
      }
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
    val indexes = SparkStartup.catalogOperator.listIndexes(Some(entityname)).get

    indexes.foreach {
      indexname => drop(indexname)
    }

    Success(null)
  }
}