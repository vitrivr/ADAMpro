package ch.unibas.dmi.dbis.adam.storage.engine

import java.sql.{Connection, DriverManager}
import java.util.Properties

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldDefinition
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object PostgresqlMetadataStorage extends MetadataStorage {
  Class.forName("org.postgresql.Driver")

  val url = AdamConfig.jdbcUrl
  AdamDialectRegistrar.register(url)

  /**
    * Opens a connection to the PostgreSQL database.
    *
    * @return
    */
  private def openConnection(): Connection = {
    DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
  }

  override def create(tablename: EntityName, fields: Seq[FieldDefinition]): Boolean = {
    log.debug("postgresql create operation")
    val structFields = fields.map {
      field => StructField(field.name, field.fieldtype.datatype)
    }.toSeq

    import SparkStartup.Implicits._
    val df = sqlContext.createDataFrame(sc.emptyRDD[Row], StructType(structFields))

    write(tablename, df, SaveMode.ErrorIfExists)

    //make fields unique
    val uniqueStmt = fields.filter(_.unique).map {
      field =>
        val fieldname = field.name
        s"""ALTER TABLE $tablename ADD UNIQUE ($fieldname)""".stripMargin
    }.mkString("; ")

    //add index to table
    val indexedStmt = fields.filter (_.indexed).map {
      field =>
        val fieldname = field.name
        s"""CREATE INDEX ON $tablename ($fieldname)""".stripMargin
    }.mkString("; ")

    //add primary key
    val pkfield = fields.filter (_.pk)
    assert(pkfield.size <= 1)
    val pkStmt = pkfield.map {
      case field =>
        val fieldname = field.name
        s"""ALTER TABLE $tablename ADD PRIMARY KEY ($fieldname)""".stripMargin
    }.mkString("; ")

    val connection = openConnection()

    connection.createStatement().executeUpdate(uniqueStmt)
    connection.createStatement().executeUpdate(indexedStmt)
    connection.createStatement().executeUpdate(pkStmt)

    true
  }

  override def read(tablename: EntityName): DataFrame = {
    log.debug("postgresql count operation")

    import SparkStartup.Implicits._
    sqlContext.read.format("jdbc").options(
      Map("url" -> url, "dbtable" -> tablename.toString(), "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword, "driver" -> "org.postgresql.Driver")
    ).load()
  }

  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append): Boolean = {
    log.debug("postgresql write operation")

    val props = new Properties()
    props.put("user", AdamConfig.jdbcUser)
    props.put("password", AdamConfig.jdbcPassword)
    props.put("driver", "org.postgresql.Driver")
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
