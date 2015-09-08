package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.SpectralLSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.FutureAction
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
trait Index{
  val indexname : IndexName
  val tablename : TableName
  protected val indexdata : DataFrame

  def getMetadata = {
    val metaBuilder = new IndexMetaStorageBuilder()
    prepareMeta(metaBuilder)
    metaBuilder.build()
  }

  private[index] def prepareMeta(metaBuilder : IndexMetaStorageBuilder) : Unit

  def scan(q: WorkingVector, options: Map[String, String]): FutureAction[Seq[TupleID]]
}


object Index {
  type IndexName = String
  type IndexTypeName = String

  private val storage = SparkStartup.indexStorage

  /**
   *
   * @param table
   * @param indexgenerator
   * @return
   */
  def createIndex(table : Table, indexgenerator : IndexGenerator) : Index = {
    val indexname = createIndexName(table.tablename, indexgenerator.indextypename)
    val index = indexgenerator.index(indexname, table.tablename, table.data)
    CatalogOperator.createIndex(indexname, table.tablename, indexgenerator.indextypename, index.getMetadata)
    storage.writeIndex(indexname, index.indexdata)
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
     indexname =  tablename + "_" + indextype + "_" + Random.nextInt(1000)
    } while(indexes.contains(tablename))

    indexname
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
   * @param indexname
   * @return
   */
  def retrieveIndex(indexname : IndexName) : Index = {
    if(!existsIndex(indexname)){
      throw new IndexNotExistingException()
    }

    val df = storage.readIndex(indexname)
    val tablename = CatalogOperator.getIndexTableName(indexname)
    val meta = CatalogOperator.getIndexMeta(indexname)

    val indextypename = CatalogOperator.getIndexTypeName(indexname)

    indextypename match {
      case "va" => VectorApproximationIndex(indexname, tablename, df, meta)
      case "lsh" => LSHIndex(indexname, tablename, df, meta)
      case "slsh" => SpectralLSHIndex(indexname, tablename, df, meta)
    }
  }

  /**
   *
   * @param tablename
   * @return
   */
  def getIndexnames(tablename : TableName) : Seq[IndexName] = {
    CatalogOperator.getIndexes(tablename)
  }
}