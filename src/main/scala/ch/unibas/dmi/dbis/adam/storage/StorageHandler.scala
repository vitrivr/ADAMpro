package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.Predicate
import ch.unibas.dmi.dbis.adam.storage.engine.Engine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class StorageHandler(val engine: Engine) extends Serializable with Logging {
  val name = engine.name

  def supports = engine.supports

  def specializes = engine.specializes

  protected val ENTITY_OPTION_NAME = "storing-" + engine.name + "-tablename"

  /**
    * Executes operation.
    *
    * @param desc description to display in log
    * @param op   operation to perform
    * @return
    */
  protected def execute[T](desc: String)(op: => Try[T]): Try[T] = {
    try {
      log.trace("performed storage handler (" + name + ") operation: " + desc)
      val res = op
      res
    } catch {
      case e: Exception =>
        log.error("error in storage handler (" + name + ") operation: " + desc, e)
        Failure(e)
    }
  }

  /**
    *
    * @param entityname
    */
  protected def getStorename(entityname: EntityName): String = {
    val tablename = CatalogOperator.getEntityOption(entityname, Some(ENTITY_OPTION_NAME)).get.get(ENTITY_OPTION_NAME)

    if (tablename.isEmpty) {
      log.error("storename missing from catalog for entity " + entityname + "; create method has not been called")
      throw new GeneralAdamException("no storename specified in catalog, no fallback")
    }

    tablename.get
  }

  /**
    *
    * @param entityname
    * @param attributes
    * @param params
    * @return
    */
  def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("create") {
      var storename = entityname

      while (engine.exists(storename).get) {
        storename = storename + Random.nextInt(999).toString
      }

      val res = engine.create(storename, attributes, params)

      if (res.isSuccess) {
        CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, storename)

        res.get.foreach {
          case (key, value) =>
            CatalogOperator.updateStorageEngineOption(name, storename, key, value)
        }

        Success(null)
      } else {
        Failure(res.failed.get)
      }
    }
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  def read(entityname: EntityName, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate] = Seq(), params: Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    execute("read") {
      val storename = getStorename(entityname)
      val options = CatalogOperator.getStorageEngineOption(name, storename).get
      engine.read(storename, attributes, predicates, options ++ params)
    }
  }

  /**
    *
    * @param entityname
    * @param df
    * @param attributes
    * @param mode
    * @param params
    * @return
    */
  def write(entityname: EntityName, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("write") {
      val storename = getStorename(entityname)
      val options = CatalogOperator.getStorageEngineOption(name, storename).get

      if (mode == SaveMode.Overwrite) {
        //TODO: on overwrite take over index structures, uniqueness, etc. (possibly call create()!)

        //overwriting
        var newStorename = ""
        do {
          newStorename = storename + "_new" + Random.nextInt(999)
        } while (engine.exists(newStorename).get)

        val res = engine.write(newStorename, df, attributes, SaveMode.ErrorIfExists, params ++ options)

        if (res.isSuccess) {
          //update name
          CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, newStorename)
          engine.drop(storename)

          val options = CatalogOperator.getStorageEngineOption(name, storename).get
          CatalogOperator.deleteStorageEngineOption(name, storename, None)

          options.foreach {
            case (key, value) =>
              CatalogOperator.updateStorageEngineOption(name, newStorename, key, value)
          }

          Success(null)
        } else {
          Failure(res.failed.get)
        }
      } else {
        //other save modes
        val res = engine.write(storename, df, attributes, mode, params ++ options)

        if (res.isSuccess) {
          Success(null)
        } else {
          Failure(res.failed.get)
        }
      }
    }
  }

  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  def drop(entityname: EntityName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("drop") {
      val res = engine.drop(getStorename(entityname))

      if (res.isSuccess) {
        CatalogOperator.deleteEntityOption(entityname, ENTITY_OPTION_NAME)
        Success(null)
      } else {
        Failure(res.failed.get)
      }
    }
  }


  override def equals(other: Any): Boolean =
    other match {
      case that: StorageHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
