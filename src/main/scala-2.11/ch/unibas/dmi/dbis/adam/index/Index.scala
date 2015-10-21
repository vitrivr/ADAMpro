package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndex
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.SpectralLSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.Tuple._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashSet
import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait Index[A <: IndexTuple]{
  val indexname : IndexName
  val tablename : TableName
  val indextypename : IndexTypeName
  val precise : Boolean //TODO: confidence score?

  protected val indexdata : DataFrame

  protected def indexToTuple : RDD[A]


  protected def getIndexTuples(filter : Option[HashSet[TupleID]]) : RDD[A] = {
    val res = indexToTuple

    if(filter.isDefined){
      res.filter(r => {
        filter.contains(r.tid)
      })
    } else {
      res
    }
  }

  private[index] def getMetadata : Serializable

  def scan(q: WorkingVector, options: Map[String, String], filter : Option[HashSet[TupleID]], queryID : Option[String] = None): HashSet[TupleID]
}


case class CacheableIndex(index : Index[_ <: IndexTuple])

object Index {
  type IndexName = String
  type IndexTypeName = IndexStructures.Value

  private val storage = SparkStartup.indexStorage

  /**
   *
   * @param table
   * @param indexgenerator
   * @return
   */
  def createIndex(table : Table, indexgenerator : IndexGenerator) :  Index[_ <: IndexTuple] = {
    val indexname = createIndexName(table.tablename, indexgenerator.indextypename)
    val rdd: RDD[IndexerTuple[WorkingVector]] = table.rows.map { x => IndexerTuple(x.getLong(0), x.getSeq[VectorBase](1) : WorkingVector) }
    val index = indexgenerator.index(indexname, table.tablename, rdd)
    storage.writeIndex(indexname, index.indexdata)
    CatalogOperator.createIndex(indexname, table.tablename, indexgenerator.indextypename, index.getMetadata)
    index
  }

  /**
   *
   * @param tablename
   * @param indextype
   * @return
   */
  private def createIndexName(tablename : TableName, indextype : IndexTypeName) : String = {
    val indexes = CatalogOperator.getIndexes(tablename)

    var indexname = ""

    do {
     indexname =  tablename + "_" + indextype.toString + "_" + Random.nextInt(1000)
    } while(indexes.contains(tablename))

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
   * @param tablename
   * @return
   */
  def getIndexnames(tablename : TableName) : Seq[IndexName] = {
    CatalogOperator.getIndexes(tablename)
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
    val df = storage.readIndex(indexname)
    val tablename = CatalogOperator.getIndexTableName(indexname)
    val meta = CatalogOperator.getIndexMeta(indexname)

    val indextypename = CatalogOperator.getIndexTypeName(indexname)

    indextypename match {
      case IndexStructures.ECP => ECPIndex(indexname, tablename, df, meta)
      case IndexStructures.LSH => LSHIndex(indexname, tablename, df, meta)
      case IndexStructures.SH => SpectralLSHIndex(indexname, tablename, df, meta)
      case IndexStructures.VAF => VectorApproximationIndex(indexname, tablename, df, meta)
      case IndexStructures.VAV => VectorApproximationIndex(indexname, tablename, df, meta)
    }
  }

  /**
   *
   * @param indexname
   * @return
   */
  def getCacheable(indexname : IndexName) : CacheableIndex = {
    val index = retrieveIndex(indexname)

    index.getIndexTuples(None)
      .setName(indexname)
      .persist(StorageLevel.MEMORY_AND_DISK)

      //.repartition(Startup.config.partitions) //TODO: loosing persistence information - bug?

    index.getIndexTuples(None).collect()

    CacheableIndex(index)
  }


  /**
   *
   * @param indexname
   * @return
   */
  def dropIndex(indexname : IndexName) : Unit = {
    CatalogOperator.dropIndex(indexname)
    storage.dropIndex(indexname)
  }

  /**
   *
   * @param tablename
   * @return
   */
  def dropIndexesForTable(tablename: TableName) : Unit = {
    val indexes = CatalogOperator.dropIndexesForTable(tablename)

    indexes.foreach {
      index => storage.dropIndex(index)
    }
  }
}

