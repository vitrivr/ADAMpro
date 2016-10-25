package org.apache.spark.sql.execution.datasources.gis

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */

import java.sql.{Types, SQLException, Connection}
import java.util.Properties

import ch.unibas.dmi.dbis.adam.datatypes.gis.{GeographyWrapperUDT, GeometryWrapperUDT}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.jdbc.{JdbcType, JdbcDialect, JdbcDialects}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StringType, DataType, MetadataBuilder}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

class DataSource extends CreatableRelationProvider with DataSourceRegister with RelationProvider with Serializable {
  override def shortName(): String = "postgis"

  /**
    *
    * @param url
    * @param props
    * @return
    */
  private def getConnection(url: String, props: Properties): Connection = {
    val connection = JdbcUtils.createConnectionFactory(url, props)()
    connection.asInstanceOf[org.postgresql.PGConnection].addDataType("geometry", classOf[org.postgis.PGgeometry])
    connection
  }

  /**
    *
    * @param sqlContext
    * @param mode
    * @param parameters
    * @param data
    * @return
    */
  override def createRelation(sqlContext: SQLContext,  mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val table = parameters.getOrElse("table", sys.error("Option 'table' not specified"))
    val partitionColumn = parameters.getOrElse("partitionColumn", null)
    val lowerBound = parameters.getOrElse("lowerBound", null)
    val upperBound = parameters.getOrElse("upperBound", null)
    val numPartitions = parameters.getOrElse("numPartitions", null)
    val props: Properties = new Properties()
    parameters.foreach(element => {
      props.put(element._1, element._2)
    })

    //register dialect
    JdbcDialects.registerDialect(new PostGisDialect(url))

    val conn : Connection = getConnection(url, props)

    try {
      var tableExists = JdbcUtils.tableExists(conn, url, table)

      if (mode == SaveMode.Ignore && tableExists) {
        //do nothing
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }

      if (mode == SaveMode.Overwrite && tableExists) {
        JdbcUtils.dropTable(conn, table)
        tableExists = false
      }

      // Create the table if the table didn't exist.
      if (!tableExists) {
        val schema = JdbcUtils.schemaString(data, url)
        val sql = s"CREATE TABLE $table ($schema)"
        val statement = conn.createStatement
        try {
          statement.executeUpdate(sql)
        } catch {
          case sqle: SQLException => throw sqle
        } finally {
          statement.close()
        }
      }
    } catch {
      case sqle: SQLException => throw sqle
    } finally {
      conn.close()
    }

    val partitionInfo = if (partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        partitionColumn,
        lowerBound.toLong,
        upperBound.toLong,
        numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    val relation = new PostGisRelation(url, table, parts, props)(sqlContext)
    relation.saveTable(data, url, table, props)
    return relation
  }

  /**
    *
    * @param sqlContext
    * @param parameters
    * @return
    */
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val url = parameters.getOrElse("url", sys.error("Option 'url' not specified"))
    val table = parameters.getOrElse("table", sys.error("Option 'table' not specified"))
    val partitionColumn = parameters.getOrElse("partitionColumn", null)
    val lowerBound = parameters.getOrElse("lowerBound", null)
    val upperBound = parameters.getOrElse("upperBound", null)
    val numPartitions = parameters.getOrElse("numPartitions", null)
    val props = new Properties() // Additional properties that we will pass to getConnection
    parameters.foreach(element => {
      props.put(element._1, element._2)
    })

    //register dialect
    JdbcDialects.registerDialect(new PostGisDialect(url))

    if (partitionColumn != null
      && (lowerBound == null || upperBound == null || numPartitions == null)) {
      sys.error("Partitioning incompletely specified")
    }

    val partitionInfo = if (partitionColumn == null) {
      null
    } else {
      JDBCPartitioningInfo(
        partitionColumn,
        lowerBound.toLong,
        upperBound.toLong,
        numPartitions.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)

    new PostGisRelation(url, table, parts, props)(sqlContext)
  }


  /**
    * Supports the PostGis dialect.
    *
    * @param url
    */
  case class PostGisDialect(url: String) extends JdbcDialect with Serializable {
    override def canHandle(url: String): Boolean = url.equals(this.url)

    override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
      if ((sqlType == 1111 && typeName.equals("geometry")) || (sqlType == 1111 && typeName.equals("geography"))) {
        Some(StringType)
      } else {
        None
      }
    }

    override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
      case x: GeometryWrapperUDT => Some(JdbcType("geometry", Types.OTHER))
      case x: GeographyWrapperUDT => Some(JdbcType("geography", Types.OTHER))
      case _ => None
    }
  }
}
