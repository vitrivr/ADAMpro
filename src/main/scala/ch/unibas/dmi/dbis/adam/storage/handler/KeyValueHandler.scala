package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.KeyValueEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2016
  */
class KeyValueHandler(private val engine: KeyValueEngine) extends StorageHandler with Logging with Serializable {
  override val name = "kv"
  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.FEATURETYPE)
  override def specializes = Seq(FieldTypes.FEATURETYPE)

  private val ENTITY_OPTION_NAME = "storing-keyvalue-bucketname"


  /**
    *
    * @param entityname
    */
  private def getBucketname(entityname: EntityName): String = {
    //TODO: not all kv-stores support buckets; adjust
    val bucketname = CatalogOperator.getEntityOption(entityname, Some(ENTITY_OPTION_NAME)).get.get(ENTITY_OPTION_NAME)

    if (bucketname.isEmpty) {
      log.error("bucketname missing from catalog for entity " + entityname + "; create method has not been called")
      throw new GeneralAdamException("no bucketname specified in catalog, no fallback")
    }

    bucketname.get
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
      var bucketname = entityname

      while (engine.exists(bucketname).get) {
        bucketname = bucketname + Random.nextInt(999).toString
      }

      engine.create(bucketname, attributes)

      CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, bucketname)
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
      engine.read(getBucketname(entityname))
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
      val bucketname = getBucketname(entityname)

      if (mode == SaveMode.Overwrite) {
        var newBucketname = ""
        do {
          newBucketname = bucketname + "-new" + Random.nextInt(999)
        } while (engine.exists(newBucketname).get)

        engine.write(newBucketname, df, mode)

        CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, newBucketname)

        engine.drop(bucketname)
      } else {
        engine.write(bucketname, df, mode)
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
      engine.drop(getBucketname(entityname))
      CatalogOperator.deleteEntityOption(entityname, ENTITY_OPTION_NAME)
      Success(null)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: KeyValueHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
