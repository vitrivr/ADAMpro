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

import scala.util.{Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class FlatFileHandler(private val engine: FileEngine) extends StorageHandler with Logging with Serializable {
  override val name = "feature"
  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.FEATURETYPE)
  override def specializes = Seq(FieldTypes.FEATURETYPE)

  private val ENTITY_OPTION_NAME = "feature-filename"

  /**
    *
    * @param entityname
    */
  private def getFilename(entityname: EntityName): String = {
    val filename = CatalogOperator.getEntityOption(entityname, Some(ENTITY_OPTION_NAME)).get.get(ENTITY_OPTION_NAME)

    if (filename.isEmpty) {
      throw new GeneralAdamException("no filename specified in catalog, no fallback")
    }

    filename.get
  }


  /**
    *
    * @param entityname
    * @param attributes
    * @param params
    * @return
    */
  override def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("create") {
      var filename = entityname

      while (engine.exists(filename).get) {
        filename = filename + Random.nextInt(999).toString
      }

      engine.create(filename, attributes)

      CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, filename)
      Success(null)
    }
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def read(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    execute("read") {
      engine.read(getFilename(entityname))
    }
  }

  /**
    *
    * @param entityname
    * @param df
    * @param mode
    * @param params
    * @return
    */
  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("write") {
      val filename = getFilename(entityname)

      val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

      if (mode == SaveMode.Overwrite) {
        var newFilename = ""
        do {
          newFilename = filename + "-new" + Random.nextInt(999)
        } while (engine.exists(newFilename).get)

        engine.write(newFilename, df, mode, allowRepartitioning)

        CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, newFilename)

        engine.drop(filename)
      } else {
        engine.write(filename, df, mode, allowRepartitioning)
      }

      Success(null)
    }
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def drop(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("drop") {
      engine.drop(getFilename(entityname))
      CatalogOperator.deleteEntityOption(entityname, ENTITY_OPTION_NAME)
      Success(null)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: FlatFileHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
