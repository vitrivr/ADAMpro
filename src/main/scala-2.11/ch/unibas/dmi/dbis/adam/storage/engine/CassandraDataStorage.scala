package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.LazyTableStorage
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.{LazyTable, Table}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SaveMode}
import com.datastax.spark.connector._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object CassandraDataStorage extends LazyTableStorage {
  val keyspace = "adamtwo"

  /**
   *
   * @param tablename
   * @return
   */
  override def readTable(tablename: TableName): Table ={
    LazyTable(tablename,  this)
  }

  /**
   *
   * @param tablename
   * @param filter
   */
  override def readFilteredTable(tablename: TableName, filter: Set[Long]): DataFrame = {
    val table = SparkStartup.sc.cassandraTable(keyspace, tablename)
    val rdd = table.where("id IN " + filter.mkString("(", ",", ")")).map(r => {
      Row(r.getLong("__adam_id"), r.getList[Float]("feature"))
    })

    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )

    SparkStartup.sqlContext.createDataFrame(rdd, schema)
  }

  /**
   *
   * @param tablename
   */
  override def readFullTable(tablename: TableName): DataFrame = {
    val table = SparkStartup.sc.cassandraTable(keyspace, tablename)
    val rdd = table.map(r => {
      Row(r.getLong("__adam_id"), r.getList[Float]("feature"))
    })

    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )

    SparkStartup.sqlContext.createDataFrame(rdd, schema)
  }

  /**
   *
   * @param tablename
   */
  override def dropTable(tablename: TableName): Unit = ???

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def writeTable(tablename: TableName, df: DataFrame, mode: SaveMode): Unit = {
    df.rdd.saveAsCassandraTable(keyspace, tablename)
  }
}
