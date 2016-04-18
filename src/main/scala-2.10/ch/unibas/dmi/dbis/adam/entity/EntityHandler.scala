package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldTypes.LONGTYPE
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, EntityNotProperlyDefinedException}
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object EntityHandler {
  val log = Logger.getLogger(getClass.getName)

  private val featureStorage = SparkStartup.featureStorage
  private val metadataStorage = SparkStartup.metadataStorage

  def exists(entityname: EntityName): Boolean = CatalogOperator.existsEntity(entityname)

  /**
    * Creates an entity.
    *
    * @param entityname
    * @param fields if fields is specified, in the metadata storage a table is created with these names, specify fields
    *               as key = name, value = SQL type
    * @return
    */
  def create(entityname: EntityName, fields: Option[Map[String, FieldDefinition]] = None)(implicit ac: AdamContext): Try[Entity] = {
    if (exists(entityname)) {
      log.error("entity " + entityname + " exists already")
      return Failure(EntityExistingException())
    }

    featureStorage.create(entityname)

    if (fields.isDefined) {
      if (fields.get.contains(FieldNames.idColumnName)) {
        log.error("entity defined with field " + FieldNames.idColumnName + ", but name is reserved")
        return Failure(EntityNotProperlyDefinedException())
      }

      if (fields.get.count { case (name, definition) => definition.pk } > 1) {
        log.error("entity defined with more than one primary key")
        return Failure(EntityNotProperlyDefinedException())
      }


      val fieldsWithId = fields.get + (FieldNames.idColumnName -> FieldDefinition(LONGTYPE, false, true, true))
      metadataStorage.create(entityname, fieldsWithId)
      CatalogOperator.createEntity(entityname, true)
      Success(Entity(entityname, featureStorage, Option(metadataStorage)))
    } else {
      CatalogOperator.createEntity(entityname, false)
      Success(Entity(entityname, featureStorage, None))
    }
  }

  /**
    * Drops an entity.
    *
    * @param entityname
    * @param ifExists
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Null] = {
    if (!exists(entityname)) {
      if (!ifExists) {
        return Failure(EntityNotExistingException())
      } else {
        Success(null)
      }
    }

    IndexHandler.dropAll(entityname)

    featureStorage.drop(entityname)

    if (CatalogOperator.hasEntityMetadata(entityname)) {
      metadataStorage.drop(entityname)
    }

    CatalogOperator.dropEntity(entityname, ifExists)
    Success(null)
  }

  /**
    * Inserts data into an entity.
    *
    * @param entityname
    * @param insertion data frame containing all columns (of both the feature storage and the metadata storage);
    *                  note that you should name the feature column as ("feature").
    * @return
    */
  def insertData(entityname: EntityName, insertion: DataFrame)(implicit ac: AdamContext): Try[Void] = {
    val entity = load(entityname).get
    //val insertionSchema = insertion.schema
    //val entitySchema = entity.schema

    //TODO: possibly compare schemas

    entity.insert(insertion)
  }

  /**
    * Loads an entity.
    *
    * @param entityname
    * @return
    */
  def load(entityname: EntityName, cache: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException())
    }

    val entity = EntityLRUCache.get(entityname)

    if (cache) {
      entity.get.getFeaturedata.rdd.setName(entityname + "_feature").cache()

      val meta = entity.get.getMetadata

      if (meta.isDefined) {
        meta.get.rdd.setName(entityname + "_feature").cache()
      }
    }

    entity
  }

  /**
    * Loads the entityname metadata without loading the data itself yet.
    *
    * @param entityname
    * @return
    */
  private[entity] def loadEntityMetaData(entityname: EntityName)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException())
    }

    val entityMetadataStorage = if (CatalogOperator.hasEntityMetadata(entityname)) {
      Option(metadataStorage)
    } else {
      None
    }

    Success(Entity(entityname, featureStorage, entityMetadataStorage))
  }


  /**
    * Returns number of tuples in entity (only feature storage is considered).
    *
    * @param entityname
    * @return the number of tuples in the entity
    */
  def countTuples(entityname: EntityName)(implicit ac: AdamContext): Int = {
    load(entityname).get.count
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list(): Seq[EntityName] = CatalogOperator.listEntities()

  /**
    *
    * @param entityname
    * @return
    */
  def getProperties(entityname: EntityName)(implicit ac: AdamContext): Try[Map[String, String]] = {
    val entity = load(entityname)

    if (entity.isSuccess) {
      Success(entity.get.getEntityProperties())
    } else {
      Failure(entity.failed.get)
    }
  }
}
