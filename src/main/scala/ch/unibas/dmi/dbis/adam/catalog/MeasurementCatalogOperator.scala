package ch.unibas.dmi.dbis.adam.catalog

import java.io.{ObjectInputStream, ByteArrayInputStream, ObjectOutputStream, ByteArrayOutputStream}

import ch.unibas.dmi.dbis.adam.catalog.catalogs._
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.query.handler.generic.QueryExpression
import ch.unibas.dmi.dbis.adam.utils.Logging
import com.mchange.v2.c3p0.ComboPooledDataSource
import slick.dbio.NoStream
import slick.driver.DerbyDriver.api._

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
object MeasurementCatalogOperator extends Logging {
  private val MAX_WAITING_TIME: Duration = 100.seconds

  private val ds = new ComboPooledDataSource
  ds.setDriverClass("org.apache.derby.jdbc.EmbeddedDriver")
  ds.setJdbcUrl("jdbc:derby:" + AdamConfig.internalsPath + "/ap_measurements" + "")

  private val DB = Database.forDataSource(ds)

  private[catalog] val SCHEMA = "adampro"
  private val _measurements = TableQuery[MeasurementCatalog]
  private[catalog] val CATALOGS = Seq(
    _measurements
  )


  /**
    * Initializes the catalog. Method is called at the beginning (see below).
    */
  private def init() {
    val connection = Database.forURL("jdbc:derby:" + AdamConfig.internalsPath + "/ap_measurements" + ";create=true")

    try {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      val schemaExists = Await.result(connection.run(sql"""SELECT COUNT(*) FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '#$SCHEMA'""".as[Int]), MAX_WAITING_TIME).headOption

      if (schemaExists.isEmpty || schemaExists.get == 0) {
        //schema might not exist yet
        actions += sqlu"""CREATE SCHEMA #$SCHEMA"""
      }

      val tables = Await.result(connection.run(sql"""SELECT TABLENAME FROM SYS.SYSTABLES NATURAL JOIN SYS.SYSSCHEMAS WHERE SCHEMANAME = '#$SCHEMA'""".as[String]), MAX_WAITING_TIME).toSeq

      CATALOGS.foreach { catalog =>
        if (!tables.contains(catalog.baseTableRow.tableName)) {
          actions += catalog.schema.create
        } else {
        }
      }

      Await.result(connection.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
    } catch {
      case e: Exception =>
        log.error("fatal error when creating catalogs", e)
        System.exit(1)
        throw new GeneralAdamException("fatal error when creating catalogs")
    } finally {
      connection.close()
    }
  }

  init()

  /**
    * Executes operation.
    *
    * @param desc description to display in log
    * @param op   operation to perform
    * @return
    */
  private def execute[T](desc: String)(op: => T): Try[T] = {
    try {
      log.trace("performed catalog operation: " + desc)
      val res = op
      Success(res)
    } catch {
      case e: Exception =>
        log.error("error in catalog operation: " + desc, e)
        Failure(e)
    }
  }

  /**
    * Adds a measurement to the catalog
    *
    * @param key
    * @param qexpr
    * @param value
    * @return
    */
  def addMeasurement(key: String, qexpr: QueryExpression, value: Long): Try[Void] = {
    execute("add measurement") {
      val serQuery = serialize(qexpr)
      val query = _measurements.+=(key, serQuery, value)
      DB.run(query)
      null
    }
  }

  /**
    *
    * @param o
    * @tparam T
    * @return
    */
  private def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /**
    *
    * @param bytes
    * @tparam T
    * @return
    */
  private def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }

  /**
    * Gets measurements for given key.
    *
    * @param key
    * @return
    */
  def getMeasurements(key: String): Try[Seq[(QueryExpression, Long)]] = {
    execute("get measurement") {
      val query = _measurements.filter(_.key === key).map(x => (x.query, x.measurement)).result
      Await.result(DB.run(query), MAX_WAITING_TIME).map(x => (deserialize[QueryExpression](x._1), x._2))
    }
  }

  /**
    * Drops measurements for given key.
    *
    * @param key
    * @return
    */
  def dropMeasurements(key: String): Try[Void] = {
    execute("drop measurements") {
      Await.result(DB.run(_measurements.filter(_.key === key).delete), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Drops measurements for given key.
    *
    * @return
    */
  def dropAllMeasurements(): Try[Void] = {
    execute("drop all measurements") {
      Await.result(DB.run(_measurements.delete), MAX_WAITING_TIME)
      null
    }
  }
}
