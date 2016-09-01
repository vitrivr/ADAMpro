package org.apache.spark.sql.execution.datasources.gis

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import ch.unibas.dmi.dbis.adam.datatypes.gis.{GeographyWrapper, GeographyWrapperUDT, GeometryWrapper, GeometryWrapperUDT}
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCRelation, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{Logging, Partition}

import scala.util.control.NonFatal

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class PostGisRelation(url: String, table: String, parts: Array[Partition], props: Properties = new Properties())(sqlContext: SQLContext)
  extends JDBCRelation(url, table, parts, props)(sqlContext) with Serializable with Logging {

  /**
    *
    * @param data
    * @param overwrite
    */
  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    data.write
      .format("ch.unibas.dmi.dbis.adam.storage.sources.gis")
      .mode(if (overwrite) SaveMode.Overwrite else SaveMode.Append)
      .option("url", props.getProperty("url"))
      .option("table", props.getProperty("table"))
      .save()
  }

  /**
    *
    * @param dt
    * @param dialect
    * @return
    */
  private def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }


  /**
    *
    * @param df
    * @param url
    * @param table
    * @param properties
    */
  def saveTable(df: DataFrame, url: String, table: String, properties: Properties) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = JdbcUtils.createConnectionFactory(url, properties)
    val batchSize = properties.getProperty("batchsize", "1000").toInt
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect)
    }
  }

  /**
    *
    * @param conn
    * @param table
    * @param rddSchema
    * @return
    */
  private def insertStatement(conn: Connection, table: String, rddSchema: StructType): PreparedStatement = {
    val columns = rddSchema.fields.map(_.name).mkString(",")
    val placeholders = rddSchema.fields.map(field => {
      if(field.dataType.isInstanceOf[GeometryWrapperUDT]){
        "ST_GeometryFromText(?)"
      } else if(field.dataType.isInstanceOf[GeographyWrapperUDT]) {
        "ST_GeographyFromText(?)"
      } else {
        "?"
      }
    }).mkString(",")
    val sql = s"INSERT INTO $table ($columns) VALUES ($placeholders)"
    conn.prepareStatement(sql)
  }


  /**
    *
    * @param getConnection
    * @param table
    * @param iterator
    * @param rddSchema
    * @param nullTypes
    * @param batchSize
    * @param dialect
    * @return
    */
  def savePartition(getConnection: () => Connection, table: String, iterator: Iterator[Row], rddSchema: StructType, nullTypes: Array[Int], batchSize: Int, dialect: JdbcDialect): Iterator[Byte] = {
    val conn = getConnection()
    var committed = false
    val supportsTransactions = try {
      conn.getMetaData().supportsDataManipulationTransactionsOnly() ||
        conn.getMetaData().supportsDataDefinitionAndDataManipulationTransactions()
    } catch {
      case NonFatal(e) =>
        logWarning("Exception while detecting transaction support", e)
        true
    }

    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
      }
      val stmt = insertStatement(conn, table, rddSchema)
      try {
        var rowCount = 0
        while (iterator.hasNext) {
          val row = iterator.next()
          val numFields = rddSchema.fields.length
          var i = 0
          while (i < numFields) {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              rddSchema.fields(i).dataType match {
                case IntegerType => stmt.setInt(i + 1, row.getInt(i))
                case LongType => stmt.setLong(i + 1, row.getLong(i))
                case DoubleType => stmt.setDouble(i + 1, row.getDouble(i))
                case FloatType => stmt.setFloat(i + 1, row.getFloat(i))
                case ShortType => stmt.setInt(i + 1, row.getShort(i))
                case ByteType => stmt.setInt(i + 1, row.getByte(i))
                case BooleanType => stmt.setBoolean(i + 1, row.getBoolean(i))
                case StringType => stmt.setString(i + 1, row.getString(i))
                case BinaryType => stmt.setBytes(i + 1, row.getAs[Array[Byte]](i))
                case TimestampType => stmt.setTimestamp(i + 1, row.getAs[java.sql.Timestamp](i))
                case DateType => stmt.setDate(i + 1, row.getAs[java.sql.Date](i))
                case t: DecimalType => stmt.setBigDecimal(i + 1, row.getDecimal(i))
                case g : GeometryWrapperUDT => stmt.setString(i + 1, row.getAs[GeometryWrapper](i).getValue)
                case g : GeographyWrapperUDT => stmt.setString(i + 1, row.getAs[GeographyWrapper](i).getValue)
                case ArrayType(et, _) =>
                  val array = conn.createArrayOf(
                    getJdbcType(et, dialect).databaseTypeDefinition.toLowerCase,
                    row.getSeq[AnyRef](i).toArray)
                  stmt.setArray(i + 1, array)
                case _ => throw new IllegalArgumentException(
                  s"Can't translate non-null value for field $i")
              }
            }
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
    Array[Byte]().iterator
  }

}