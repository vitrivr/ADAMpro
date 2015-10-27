package ch.unibas.dmi.dbis.adam.table

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.exception.{TableCreationException, TableNotExistingException}
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import scala.collection.immutable.HashSet
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
abstract class Table {
  def tablename : TableName
  def count : Long

  def show() : Array[Row]
  def show(n : Int) : Array[Row]

  def rows : RDD[Row]

  def tuples  : RDD[Tuple]
  def tuplesForKeys(filter: HashSet[Long]): RDD[Tuple]

  def getData : DataFrame

  def getMetadata : DataFrame
}


object Table {
  type TableName = String

  private val sqlContext = SparkStartup.sqlContext
  private val tableStorage = SparkStartup.tableStorage
  private val metadataStorage = SparkStartup.metadataStorage


  /**
   *
   * @param tablename
   * @return
   */
  def existsTable(tablename : TableName) : Boolean = {
    CatalogOperator.existsTable(tablename)
  }

  /**
   *
   * @param tablename
   * @param schema
   * @return
   */
  def createTable(tablename : TableName, schema : StructType) : Table = {
    val fields = schema.fields

    require(fields.filter(_.name == "feature").length <= 1)

    CatalogOperator.createTable(tablename)

    val tableSchema = StructType(
      List(
        StructField("__adam_id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )
    val tableData = sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], tableSchema)


    val metadataSchema = StructType(
      List(StructField("__adam_id", LongType, false)) ::: fields.filterNot(_.name == "feature").toList
    )
    val metadataData = sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], metadataSchema)


    val future = Future {
        tableStorage.writeTable(tablename, tableData, SaveMode.ErrorIfExists)
        metadataStorage.writeTable(tablename, metadataData, SaveMode.ErrorIfExists)
      }
    future onFailure {case t => new TableCreationException()}

    Await.ready(future, Duration.Inf)

    DefaultTable(tablename, tableData, metadataData)
  }

  /**
   *
   * @param tablename
   * @param ifExists
   */
  def dropTable(tablename : TableName, ifExists : Boolean = false) : Unit = {
    val indexes = CatalogOperator.getIndexes(tablename)
    CatalogOperator.dropTable(tablename, ifExists)

    tableStorage.dropTable(tablename)
  }

  /**
   *
   * @param tablename
   * @return
   */
  def insertData(tablename : TableName, insertion: DataFrame): Unit ={
    if(!existsTable(tablename)){
      throw new TableNotExistingException()
    }

    val future = Future {
        tableStorage.writeTable(tablename, insertion, SaveMode.Append)
    }

    Await.ready(future, Duration.Inf)
  }

  /**
   *
   * @param tablename
   * @return
   */
  def retrieveTable(tablename : TableName) : Table = {
    if(!existsTable(tablename)){
      throw new TableNotExistingException()
    }

    if(RDDCache.containsTable(tablename)){
      RDDCache.getTable(tablename)
    } else {
      DefaultTable(tablename, tableStorage.readTable(tablename), metadataStorage.readTable(tablename))
    }
  }

  /**
   *
   * @param tablename
   * @return
   */
  def getCacheable(tablename : TableName) : CacheableTable = {
    val table = Table.retrieveTable(tablename)

    table.tuples
      .repartition(Startup.config.partitions)
      .setName(tablename).persist(StorageLevel.MEMORY_AND_DISK)
      .collect()

    CacheableTable(table)
  }
}
