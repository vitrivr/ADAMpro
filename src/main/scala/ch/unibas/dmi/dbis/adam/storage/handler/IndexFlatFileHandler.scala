package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.FileEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class IndexFlatFileHandler(private val engine: FileEngine) extends StorageHandler with Logging with Serializable {
  override val name: String = "index"

  override def supports = Seq()

  override def specializes: Seq[FieldType] = Seq()

  override def create(indexname: IndexName, attributes: Seq[AttributeDefinition], params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    Success(null)
  }

  override def read(indexname: IndexName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      var filename = CatalogOperator.getIndexOption(indexname, "filename").getOrElse(indexname.toString)
      engine.read(filename)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def write(indexname: IndexName, df: DataFrame, mode: SaveMode = SaveMode.ErrorIfExists, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      var filename = CatalogOperator.getIndexOption(indexname, "filename").getOrElse(indexname.toString)

      val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

      if (mode == SaveMode.Overwrite) {
        var newFilename = ""
        do {
          newFilename = filename + "-new" + Random.nextInt(999)
        } while (engine.exists(newFilename).get)

        engine.write(newFilename, df, mode, allowRepartitioning)

        CatalogOperator.updateIndexOption(indexname, "filename", newFilename)

        engine.drop(filename)
      } else {
        engine.write(filename, df, mode)
      }
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }


  override def drop(indexname: IndexName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      var filename = CatalogOperator.getIndexOption(indexname, "filename").getOrElse(indexname.toString)
      engine.drop(filename)
      CatalogOperator.deleteIndexOption(indexname, "filename")
      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }


  override def equals(other: Any): Boolean =
    other match {
      case that: IndexFlatFileHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
