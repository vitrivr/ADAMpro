package ch.unibas.dmi.dbis.adam.storage.engine

import java.sql.{Connection, DriverManager}
import java.util.Properties

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.MetadataStorage
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object PostgresqlMetadataStorage extends MetadataStorage {
  Class.forName("org.postgresql.Driver")

  val url = AdamConfig.jdbcUrl

  /**
    * Opens a connection to the PostgreSQL database.
    *
    * @return
    */
  private def openConnection(): Connection = {
    DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
  }

  override def create(entityname: EntityName, fields: Seq[AttributeDefinition]): Try[Option[String]] = {
    try {
      log.debug("postgresql create operation")
      val structFields = fields.map {
        field => StructField(field.name, field.fieldtype.datatype)
      }.toSeq

      import SparkStartup.Implicits._
      val df = sqlContext.createDataFrame(sc.emptyRDD[Row], StructType(structFields))

      val tablename = write(entityname, df, SaveMode.ErrorIfExists)

      if(tablename.isFailure){
        return Failure(tablename.failed.get)
      }

      //make fields unique
      val uniqueStmt = fields.filter(_.unique).map {
        field =>
          val fieldname = field.name
          s"""ALTER TABLE $entityname ADD UNIQUE ($fieldname)""".stripMargin
      }.mkString("; ")

      //add index to table
      val indexedStmt = fields.filter(_.indexed).map {
        field =>
          val fieldname = field.name
          s"""CREATE INDEX ON $entityname ($fieldname)""".stripMargin
      }.mkString("; ")

      //add primary key
      val pkfield = fields.filter(_.pk)
      assert(pkfield.size <= 1)
      val pkStmt = pkfield.map {
        case field =>
          val fieldname = field.name
          s"""ALTER TABLE $entityname ADD PRIMARY KEY ($fieldname)""".stripMargin
      }.mkString("; ")

      val connection = openConnection()

      connection.createStatement().executeUpdate(uniqueStmt)
      connection.createStatement().executeUpdate(indexedStmt)
      connection.createStatement().executeUpdate(pkStmt)

      Success(Some(tablename.get))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def read(tablename: EntityName): Try[DataFrame] = {
    log.debug("postgresql read operation")

    try {
      import SparkStartup.Implicits._
      val df = sqlContext.read.format("jdbc").options(
        Map("url" -> url, "dbtable" -> tablename.toString(), "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword, "driver" -> "org.postgresql.Driver")
      ).load()
      Success(df)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def write(tablename: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append): Try[String] = {
    log.debug("postgresql write operation")

    try {
      val props = new Properties()
      props.put("user", AdamConfig.jdbcUser)
      props.put("password", AdamConfig.jdbcPassword)
      props.put("driver", "org.postgresql.Driver")
      df.write.mode(mode).jdbc(url, tablename, props)

      Success(tablename)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  override def drop(tablename: EntityName): Try[Void] = {
    log.debug("postgresql drop operation")

    try {
      val dropTableSql = s"""DROP TABLE $tablename""".stripMargin
      openConnection().createStatement().executeUpdate(dropTableSql)
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
