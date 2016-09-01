package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.RelationalDatabaseEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Success, Random, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class DatabaseHandler(private val engine: RelationalDatabaseEngine) extends StorageHandler with Logging with Serializable {
  override val name = "relational"

  override def supports: Seq[FieldType] = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)

  override def specializes: Seq[FieldType] = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)

  protected val ENTITY_OPTION_NAME = "storing-relational-tablename"


  /**
    *
    * @param entityname
    */
  protected def getTablename(entityname: EntityName): String = {
    val tablename = CatalogOperator.getEntityOption(entityname, Some(ENTITY_OPTION_NAME)).get.get(ENTITY_OPTION_NAME)

    if (tablename.isEmpty) {
      log.error("tablename missing from catalog for entity " + entityname + "; create method has not been called")
      throw new GeneralAdamException("no tablename specified in catalog, no fallback")
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
  override def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("create") {
      var tablename = entityname

      while (engine.exists(tablename).get) {
        tablename = tablename + Random.nextInt(999).toString
      }

      val res = engine.create(tablename, attributes)

      val tmp = ENTITY_OPTION_NAME

      if (res.isSuccess) {
        CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, tablename)
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
  override def read(entityname: EntityName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    execute("read") {
      engine.read(getTablename(entityname))
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
  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("write") {
      val tablename = getTablename(entityname)

      if (mode == SaveMode.Overwrite) {
        //overwriting
        var newTablename = ""
        do {
          newTablename = tablename + "-new" + Random.nextInt(999)
        } while (engine.exists(newTablename).get)

        val res = engine.write(newTablename, df)


        if (res.isSuccess) {
          //update name
          CatalogOperator.updateEntityOption(entityname, ENTITY_OPTION_NAME, newTablename)
          engine.drop(tablename)
          Success(null)
        } else {
          Failure(res.failed.get)
        }
      } else {
        //other save modes
        val res = engine.write(tablename, df)

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
  override def drop(entityname: EntityName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    execute("drop") {
      val res = engine.drop(getTablename(entityname))

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
      case that: DatabaseHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
