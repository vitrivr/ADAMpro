package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.FileEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class FlatFileHandler(private val engine: FileEngine) extends StorageHandler with Logging with Serializable {
  override val name: String = "feature"
  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.FEATURETYPE)
  override def specializes = Seq(FieldTypes.FEATURETYPE)

  override def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      var filename = entityname

      while (engine.exists(filename).get) {
        filename = filename + Random.nextInt(999).toString
      }

      engine.create(filename, attributes)

      CatalogOperator.updateEntityOption(entityname, "filename", filename)
      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def read(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      val filename = CatalogOperator.getEntityOption(entityname, "filename")

      if (filename.isEmpty) {
        log.warn("no filename specified in catalog, fallback to entityname")
      }

      engine.read(filename.get)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      val filename = CatalogOperator.getEntityOption(entityname, "filename")

      if (filename.isEmpty) {
        throw new GeneralAdamException("no filename specified in catalog, no fallback used")
      }

      val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

      if (mode == SaveMode.Overwrite) {
        var newFilename = ""
        do {
          newFilename = filename.get + "-new" + Random.nextInt(999)
        } while (engine.exists(newFilename).get)

        engine.write(newFilename, df, mode, allowRepartitioning)

        CatalogOperator.updateEntityOption(entityname, "filename", newFilename)

        engine.drop(filename.get)
      } else {
        engine.write(filename.get, df, mode, allowRepartitioning)
      }
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def drop(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      val filename = CatalogOperator.getEntityOption(entityname, "filename")

      if (filename.isEmpty) {
        throw new GeneralAdamException("no filename specified in catalog, no fallback used")
      }

      engine.drop(filename.get)
      CatalogOperator.deleteEntityOption(entityname, "filename")

      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: FlatFileHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
