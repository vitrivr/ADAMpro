package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.RelationalDatabaseEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.{Failure, Random, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class DatabaseHandler(private val engine : RelationalDatabaseEngine) extends StorageHandler with Logging with Serializable {
  override val name: String = "relational"
  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)
  override def specializes = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE)


  override def create(entityname: EntityName, attributes: Seq[AttributeDefinition], params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      var tablename = entityname

      while (engine.exists(tablename).get) {
        tablename = tablename + Random.nextInt(999).toString
      }

      engine.create(tablename, attributes)

      CatalogOperator.updateEntityOption(entityname, "tablename", tablename)
      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def read(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      val tablename = CatalogOperator.getEntityOption(entityname, "tablename")

      if (tablename.isEmpty) {
        log.warn("no tablename specified in catalog, fallback to entityname")
      }

      engine.read(tablename.getOrElse(entityname))
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      val tablename = CatalogOperator.getEntityOption(entityname, "tablename")

      if (tablename.isEmpty) {
        throw new GeneralAdamException("no tablename specified in catalog, no fallback used")
      }

      if (mode == SaveMode.Overwrite) {
        var newTablename = ""
        do {
          newTablename = tablename.get + "-new" + Random.nextInt(999)
        } while (engine.exists(newTablename).get)

        engine.write(newTablename, df)

        CatalogOperator.updateEntityOption(entityname, "tablename", newTablename)

        engine.drop(tablename.get)
      } else {
        engine.write(tablename.get, df)
      }
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }


  override def drop(entityname: EntityName, params : Map[String, String] = Map())(implicit ac: AdamContext): Try[Void] = {
    try {
      val filename = CatalogOperator.getEntityOption(entityname, "tablename")

      if (filename.isEmpty) {
        throw new GeneralAdamException("no tablename specified in catalog, no fallback used")
      }

      engine.drop(filename.get)
      CatalogOperator.deleteEntityOption(entityname, "tablename")

      Success(null)
    } catch {
      case e: Exception =>
        Failure(e)
    }
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: DatabaseHandler => this.engine.equals(that.engine)
      case _ => false
    }

  override def hashCode: Int = engine.hashCode
}
