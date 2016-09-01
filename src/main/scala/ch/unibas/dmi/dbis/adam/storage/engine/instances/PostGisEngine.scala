package ch.unibas.dmi.dbis.adam.storage.engine.instances

import java.sql.Connection

import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.GisDatabaseEngine
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class PostGisEngine(private val url: String, private val user: String, private val password: String) extends PostgresqlEngine(url, user, password, "public") with GisDatabaseEngine {
  //TODO: gis functions only available in the public schema

  /**
    * Opens a connection to a PostGIS database.
    *
    * @return
    */
  override protected def openConnection(): Connection = {
    val connection = super.openConnection()
    connection.asInstanceOf[org.postgresql.PGConnection].addDataType("geometry", classOf[org.postgis.PGgeometry])
    connection
  }

  /**
    *
    * @param tablename name of table
    * @param df
    * @param mode
    * @return
    */
  override def write(tablename: String, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Try[Void] = {
    log.debug("postgresql write operation")

    try {
      df.write.mode(mode)
        .format("org.apache.spark.sql.execution.datasources.gis.DataSource")
        .options(propsMap ++ Seq("table" -> tablename))
        .save
      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }
}