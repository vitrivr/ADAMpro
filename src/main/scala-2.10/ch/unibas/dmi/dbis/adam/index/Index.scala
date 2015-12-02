package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndex
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndex
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait Index[A <: IndexTuple]{
  val indextypename : IndexTypeName

  val indexname : IndexName
  val entityname : EntityName
  val confidence : Float

  protected val df : DataFrame
  protected def rdd : RDD[A]
  protected def rdd(filter : Option[HashSet[TupleID]]) : RDD[A] = if(filter.isDefined){ rdd.filter(t => filter.get.contains(t.tid)) } else { rdd }

  private[index] def metadata : Serializable

  def scan(q : FeatureVector, options: Map[String, Any], k : Int, filter : Option[HashSet[TupleID]], queryID : Option[String] = None) : HashSet[TupleID] = {
    SparkStartup.sc.setLocalProperty("spark.scheduler.pool", "index")
    SparkStartup.sc.setJobGroup(queryID.getOrElse(""), indextypename.toString, true)

    scan(rdd(filter), q, options, k)
  }

  def scan(data : RDD[A], q: FeatureVector, options: Map[String, Any], k : Int): HashSet[TupleID]
}


case class CacheableIndex(index : Index[_ <: IndexTuple])

object Index {
  type IndexName = String
  type IndexTypeName = IndexStructures.Value

  private val storage = SparkStartup.indexStorage

  /**
   *
   * @param entity
   * @param indexgenerator
   * @return
   */
  def createIndex(entity : Entity, indexgenerator : IndexGenerator) :  Index[_ <: IndexTuple] = {
    val indexname = createIndexName(entity.entityname, indexgenerator.indextypename)
    val rdd: RDD[IndexerTuple] = entity.featuresRDD.map { x => IndexerTuple(x.getLong(0), x.getAs[FeatureVectorWrapper](1).value) }
    val index = indexgenerator.index(indexname, entity.entityname, rdd)
    storage.write(indexname, index.df)
    CatalogOperator.createIndex(indexname, entity.entityname, indexgenerator.indextypename, index.metadata)
    index
  }

  /**
   *
   * @param entityname
   * @param indextype
   * @return
   */
  private def createIndexName(entityname : EntityName, indextype : IndexTypeName) : String = {
    val indexes = CatalogOperator.getIndexes(entityname)

    var indexname = ""

    var i = 0
    do {
     indexname =  entityname + "_" + indextype.toString + "_" + i
      i += 1
    } while(indexes.contains(indexname))

    indexname
  }

  /**
   *
   * @param indexname
   * @return
   */
  def existsIndex(indexname : IndexName) : Boolean = {
    CatalogOperator.existsIndex(indexname)
  }

  /**
   *
   * @param entityname
   * @return
   */
  def getIndexnames(entityname : EntityName) : Seq[IndexName] = {
    CatalogOperator.getIndexes(entityname)
  }

  /**
   *
   * @return
   */
  def getIndexnames() : Seq[IndexName] = {
    CatalogOperator.getIndexes()
  }

  /**
   *
   * @param indexname
   * @return
   */
  def retrieveIndexConfidence(indexname : IndexName) : Float = {
    retrieveIndex(indexname).confidence
  }

  /**
   *
   * @param indexname
   * @return
   */
  def retrieveIndex(indexname : IndexName) :  Index[_ <: IndexTuple] = {
    if(!existsIndex(indexname)){
      throw new IndexNotExistingException()
    }

    if(RDDCache.containsIndex(indexname)){
      RDDCache.getIndex(indexname)
    } else {
      loadCacheMissedIndex(indexname)
    }
  }

  /**
   *
   * @param indexname
   * @return
   */
  private def loadCacheMissedIndex(indexname : IndexName) : Index[_ <: IndexTuple] = {
    val df = storage.read(indexname)
    val entityname = CatalogOperator.getIndexEntity(indexname)
    val meta = CatalogOperator.getIndexMeta(indexname)

    val indextypename = CatalogOperator.getIndexTypeName(indexname)

    indextypename match {
      case IndexStructures.ECP => ECPIndex(indexname, entityname, df, meta)
      case IndexStructures.LSH => LSHIndex(indexname, entityname, df, meta)
      case IndexStructures.SH => SHIndex(indexname, entityname, df, meta)
      case IndexStructures.VAF => VAIndex(indexname, entityname, df, meta)
      case IndexStructures.VAV => VAIndex(indexname, entityname, df, meta)
    }
  }

  /**
   *
   * @param indexname
   * @return
   */
  def getCacheable(indexname : IndexName) : CacheableIndex = {
    val index = retrieveIndex(indexname)

    index.rdd(None)
      .setName(indexname)
      .persist(StorageLevel.MEMORY_AND_DISK)

    index.rdd(None).collect()

    CacheableIndex(index)
  }


  /**
   *
   * @param indexname
   * @return
   */
  def dropIndex(indexname : IndexName) : Unit = {
    CatalogOperator.dropIndex(indexname)
    storage.drop(indexname)
  }

  /**
   *
   * @param entityname
   * @return
   */
  def dropIndexesForTable(entityname: EntityName) : Unit = {
    val indexes = CatalogOperator.dropIndexesForEntity(entityname)

    indexes.foreach {
      index => storage.drop(index)
    }
  }
}

