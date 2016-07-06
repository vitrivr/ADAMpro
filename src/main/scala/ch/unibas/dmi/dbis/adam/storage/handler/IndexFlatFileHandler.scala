package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.FileEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class IndexFlatFileHandler(private val engine: FileEngine) extends StorageHandler with Logging with Serializable {
  override val name = "index"
  override def supports = Seq()
  override def specializes: Seq[FieldType] = Seq()

  private val INDEX_OPTION_NAME = "storing-feature-filename"

  /**
    *
    * @param indexname
    */
  private def getFilename(indexname: IndexName): String = {
    var filename = CatalogOperator.getIndexOption(indexname, Some(INDEX_OPTION_NAME)).get.get(INDEX_OPTION_NAME)

    if (filename.isEmpty) {
      log.error("filename missing from catalog for index " + indexname + "; create method has not been called")
      throw new GeneralAdamException("no filename specified in catalog, no fallback")
    }

    filename.get
  }

  /**
    *
    * @param indexname
    * @param attributes
    * @param params
    * @return
    */
  override def create(indexname: IndexName, attributes: Seq[AttributeDefinition], params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("create") {
      var filename = indexname
      CatalogOperator.updateIndexOption(indexname, INDEX_OPTION_NAME, filename)
    }
  }

  /**
    *
    * @param indexname
    * @param params
    * @return
    */
  override def read(indexname: IndexName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    execute("read") {
      engine.read(getFilename(indexname))
    }
  }

  /**
    *
    * @param indexname
    * @param df
    * @param mode
    * @param params
    * @return
    */
  override def write(indexname: IndexName, df: DataFrame, mode: SaveMode = SaveMode.ErrorIfExists, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("write") {
      var filename = getFilename(indexname)

      val allowRepartitioning = params.getOrElse("allowRepartitioning", "false").toBoolean

      if (mode == SaveMode.Overwrite) {
        var newFilename = ""
        do {
          newFilename = filename + "-new" + Random.nextInt(999)
        } while (engine.exists(newFilename).get)

        engine.write(newFilename, df, mode, allowRepartitioning)

        CatalogOperator.updateIndexOption(indexname, INDEX_OPTION_NAME, newFilename)

        engine.drop(filename)
      } else {
        engine.write(filename, df, mode)
      }

      Success(null)
    }
  }


  /**
    *
    * @param indexname
    * @param params
    * @return
    */
  override def drop(indexname: IndexName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("drop") {
      engine.drop(getFilename(indexname))
      CatalogOperator.deleteIndexOption(indexname, INDEX_OPTION_NAME)
      Success(null)
    }
  }


  override def equals(other: Any): Boolean =
    other match {
      case that: IndexFlatFileHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
