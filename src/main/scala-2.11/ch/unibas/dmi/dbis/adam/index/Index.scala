package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.data.IndexMeta
import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class Index(indexname : IndexName, tablename : TableName, index: DataFrame, indexMeta: IndexMeta)

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
    val indexname = createIndexName(table.tablename, indexgenerator.indexname)
    val index = indexgenerator.index(indexname, table.tablename, table.data)
    CatalogOperator.createIndex(indexname, table.tablename, index.indexMeta)
    storage.writeIndex(indexname, index.index)
    index
  }

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
    val meta = CatalogOperator.getIndexMeta(indexname)

    Index(indexname, CatalogOperator.getIndexTableName(indexname), df, meta)
  }


}