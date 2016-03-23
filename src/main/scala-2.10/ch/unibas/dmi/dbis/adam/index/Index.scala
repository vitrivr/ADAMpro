package ch.unibas.dmi.dbis.adam.index

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndex
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndex
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait Index[A <: IndexTuple] {
  @transient lazy val log = Logger.getLogger(getClass.getName)

  val indexname: IndexName
  val indextype: IndexTypeName
  val entityname: EntityName

  //confidence towards the index. Confidence of 1 means very confident in index results (i.e., precise results).
  val confidence: Float

  /**
    *
    */
  protected val df: DataFrame

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
  def isQueryConform(nnq : NearestNeighbourQuery) : Boolean

  /**
    * Scans the index.
    *
    * @param q query vector
    * @param options options to be passed to the index reader
    * @param k number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @param filter optional pre-filter for Boolean query
    * @param queryID optional query id
    * @return a set of candidate tuple ids
    */
  def scan(q: FeatureVector, options: Map[String, Any], k: Int, filter: Option[Set[TupleID]], queryID: Option[String] = None): Set[TupleID] = {
    log.debug("started scanning index")
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "index")
    SparkStartup.sc.setJobGroup(queryID.getOrElse(""), indextype.name, true)

    val data = if (filter.isDefined) {
      df.filter(df(FieldNames.idColumnName) isin (filter.get.toSeq : _*))
    } else {
      df
    }

    scan(data,q, options, k)
  }

  /**
    * Scans the index.
    *
    * @param data rdd to scan
    * @param q query vector
    * @param options options to be passed to the index reader
    * @param k number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids
    */
  protected def scan(data: DataFrame, q: FeatureVector, options: Map[String, Any], k: Int): Set[TupleID]
}


object Index {
  type IndexName = String
  type IndexTypeName = IndexTypes.IndexType

  private val storage = SparkStartup.indexStorage


  /**
    * Creates an index that is unique and which folows the naming rules of indexes.
    *
    * @param entityname
    * @param indextype
    * @return
    */
  private def createIndexName(entityname: EntityName, indextype: IndexTypeName): String = {
    val indexes = CatalogOperator.listIndexes(entityname, indextype).map(_._1)

    var indexname = ""

    var i = indexes.length
    do {
      indexname = entityname + "_" + indextype.name + "_" + i
      i += 1
    } while (indexes.contains(indexname))

    indexname
  }


  /**
    * Creates an index.
    *
    * @param entity
    * @param indexgenerator generator to create index
    * @return index
    */
  def createIndex(entity: Entity, indexgenerator: IndexGenerator): Index[_ <: IndexTuple] = {
    val indexname = createIndexName(entity.entityname, indexgenerator.indextypename)
    val rdd: RDD[IndexingTaskTuple] = entity.getFeaturedata.map { x => IndexingTaskTuple(x.getLong(0), x.getAs[FeatureVectorWrapper](1).vector) }
    val index = indexgenerator.index(indexname, entity.entityname, rdd)
    val df = index.df.withColumnRenamed("id", FieldNames.idColumnName).withColumnRenamed("value", FieldNames.featureIndexColumnName)
    storage.write(indexname, df)
    CatalogOperator.createIndex(indexname, entity.entityname, indexgenerator.indextypename, index.metadata)
    index
  }


  /**
    * Checks whether index exists.
    *
    * @param indexname
    * @return
    */
  def exists(indexname: IndexName): Boolean = CatalogOperator.existsIndex(indexname)

  /**
    * Lists indexes.
    *
    * @param entityname
    * @param indextypename
    * @return
    */
  def list(entityname: EntityName = null, indextypename: IndexTypeName = null): Seq[(IndexName, IndexTypeName)] = CatalogOperator.listIndexes(entityname, indextypename)

  /**
    * Gets confidence score of index.
    *
    * @param indexname
    * @return
    */
  def confidence(indexname: IndexName): Float = loadIndexMetaData(indexname).confidence

  /**
    * Gets the type of the index.
    *
    * @param indexname
    * @return
    */
  def indextype(indexname: IndexName): IndexTypeName = loadIndexMetaData(indexname).indextype

  /**
    * Loads index.
    *
    * @param indexname
    * @param cache if cache is true, the index is added to the cache and read from there
    * @return
    */
  def load(indexname: IndexName): Index[_ <: IndexTuple] = {
    if (!exists(indexname)) {
      throw new IndexNotExistingException()
    }

    IndexLRUCache.get(indexname)
  }

  /**
    * Loads the index metadata without loading the data itself yet.
    *
    * @param indexname
    * @return
    */
  def loadIndexMetaData(indexname: IndexName): Index[_ <: IndexTuple] = {
    if (!exists(indexname)) {
      throw new IndexNotExistingException()
    }

    val df = storage.read(indexname)
    val entityname = CatalogOperator.getEntitynameFromIndex(indexname)
    val meta = CatalogOperator.getIndexMeta(indexname)

    val indextypename = CatalogOperator.getIndexTypeName(indexname)

    indextypename match {
      case IndexTypes.ECPINDEX => ECPIndex(indexname, entityname, df, meta)
      case IndexTypes.LSHINDEX => LSHIndex(indexname, entityname, df, meta)
      case IndexTypes.SHINDEX => SHIndex(indexname, entityname, df, meta)
      case IndexTypes.VAFINDEX => VAIndex(indexname, entityname, df, meta)
      case IndexTypes.VAVINDEX => VAIndex(indexname, entityname, df, meta)
    }
  }

  /**
    * Drops an index.
    *
    * @param indexname
    * @return true if index was dropped
    */
  def drop(indexname: IndexName): Boolean = {
    CatalogOperator.dropIndex(indexname)
    storage.drop(indexname)
  }

  /**
    * Drops all indexes for a given entity.
    *
    * @param entityname
    * @return
    */
  def dropAll(entityname: EntityName): Boolean = {
    val indexes = CatalogOperator.dropAllIndexes(entityname)

    indexes.foreach {
      index => storage.drop(index)
    }

    true
  }


  object IndexLRUCache {
    private val maximumCacheSizeIndex = AdamConfig.maximumCacheSizeIndex
    private val expireAfterAccess = AdamConfig.maximumCacheSizeIndex

    private val indexCache = CacheBuilder.
      newBuilder().
      maximumSize(maximumCacheSizeIndex).
      expireAfterAccess(expireAfterAccess, TimeUnit.MINUTES).
      build(
        new CacheLoader[IndexName, Index[_ <: IndexTuple]]() {
          def load(indexname: IndexName): Index[_ <: IndexTuple] = {
            val index = Index.loadIndexMetaData(indexname)
            index.df.rdd.setName(indexname).persist(StorageLevel.MEMORY_AND_DISK)
            index.df.rdd.collect()
            index
          }
        }
      )

    /**
      * Gets index from cache. If index is not yet in cache, it is loaded.
      *
      * @param indexname
      */
    def get(indexname: IndexName): Index[_ <: IndexTuple] = {
      indexCache.get(indexname)
    }
  }

}





