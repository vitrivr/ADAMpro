package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.datatypes.Feature.{StoredVector, _}
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.LazyTableStorage
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.{LazyTable, Table}
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object PostgresDataStorage extends LazyTableStorage {
  val config = Startup.config
  val url = config.jdbcUrl
  AdamDialectRegistrar.register(url)

  /**
   *
   * @param tablename
   * @return
   */
  override def readTable(tablename: TableName): Table = {
    LazyTable(tablename,  this)
  }

  /**
   *
   * @param tablename
   * @param filter
   */
  override def readFilteredTable(tablename: TableName, filter: Set[Long]): DataFrame = {
    val df = SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> config.jdbcUser, "password" -> config.jdbcPassword)
    ).load()

    val adamIDIdx = df.schema.fieldIndex("__adam_id")
    val featureIdx = df.schema.fieldIndex("feature")

    val adaptedRDD = df.filter("id IN " + filter.mkString("(", ",", ")")).map(r => Row(r.getLong(adamIDIdx), r.getString(featureIdx) : StoredVector))

    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )

    SparkStartup.sqlContext.createDataFrame(adaptedRDD, schema)
  }

  /**
   *
   * @param tablename
   */
  override def readFullTable(tablename: TableName): DataFrame = {
    val df = SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> config.jdbcUser, "password" -> config.jdbcPassword)
    ).load()

    val adamIDIdx = df.schema.fieldIndex("__adam_id")
    val featureIdx = df.schema.fieldIndex("feature")

    val adaptedRDD = df.map(r => Row(r.getLong(adamIDIdx), r.getString(featureIdx) : StoredVector))

    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )

    SparkStartup.sqlContext.createDataFrame(adaptedRDD, schema)
  }

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def writeTable(tablename: TableName, df: DataFrame, mode: SaveMode): Unit = ???

  /**
   *
   * @param tablename
   */
  override def dropTable(tablename: TableName): Unit = ???
}
