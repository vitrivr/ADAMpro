package ch.unibas.dmi.dbis.adam.storage.engine

import java.sql.{Connection, DriverManager}
import java.util.Properties

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import org.apache.log4j.Logger
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object PostgresqlMetadataStorage extends MetadataStorage {
  val log = Logger.getLogger(getClass.getName)

  val url = AdamConfig.jdbcUrl
  AdamDialectRegistrar.register(url)

  /**
    * Opens a connection to the PostgreSQL database.
    *
    * @return
    */
  private def openConnection(): Connection ={
    Class.forName("org.postgresql.Driver").newInstance
    DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
  }

  override def create(tablename: EntityName, fields: Map[String, DataType]): Boolean = {
    log.debug("postgresql create operation")
    val structFields = fields.map{
      case (name, fieldtype) => StructField(name, fieldtype)
    }.toSeq

    val df = SparkStartup.sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], StructType(structFields))

    write(tablename, df, SaveMode.ErrorIfExists)

    //make id field unique
    val idColumnName = FieldNames.idColumnName
    val alterAddIdSql = s"""
                   |ALTER TABLE $tablename
                   | ADD UNIQUE ($idColumnName)
                   |""".stripMargin

    openConnection().createStatement().executeUpdate(alterAddIdSql)

    //crate index over id field
    val idColumnIndexSql = s"""CREATE INDEX ON $tablename ($idColumnName)""".stripMargin

    openConnection().createStatement().executeUpdate(idColumnIndexSql)

    true
  }

  override def read(tablename: EntityName): DataFrame = {
    log.debug("postgresql count operation")

    SparkStartup.sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename, "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword)
    ).load()
  }

  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append): Boolean = {
    log.debug("postgresql write operation")

    val props = new Properties()
    props.put("user", AdamConfig.jdbcUser)
    props.put("password", AdamConfig.jdbcPassword)
    df.write.mode(mode).jdbc(url, tablename, props)
    true
  }

  override def drop(tablename: EntityName): Boolean = {
    log.debug("postgresql drop operation")

    val dropTableSql = s"""DROP TABLE $tablename""".stripMargin
    openConnection().createStatement().executeUpdate(dropTableSql)
    true
  }
}
