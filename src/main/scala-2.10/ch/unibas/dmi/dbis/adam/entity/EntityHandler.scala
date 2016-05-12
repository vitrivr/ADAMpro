package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldTypes._
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

  private val lock = new Object()

  /**
    * Creates an entity.
    *
    * @param entityname
    * @param fields if fields is specified, in the metadata storage a table is created with these names, specify fields
    *               as key = name, value = SQL type, note the reserved names
    * @return
    */
  def create(entityname: EntityName, fields: Seq[FieldDefinition])(implicit ac: AdamContext): Try[Entity] = {
    try {
      lock.synchronized {
        //checks
        if (exists(entityname)) {
          return Failure(EntityExistingException())
        }

        if (fields.isEmpty) {
          return Failure(EntityNotProperlyDefinedException(Some("Entity " + entityname + " will have no fields")))
        }

        FieldNames.reservedNames.foreach { reservedName =>
          if (fields.contains(reservedName)) {
            return Failure(EntityNotProperlyDefinedException(Some("Entity defined with field " + reservedName + ", but name is reserved")))
          }
        }

        if (fields.map(_.name).distinct.length != fields.length) {
          return Failure(EntityNotProperlyDefinedException(Some("Entity defined with duplicate fields.")))
        }

        val allowedPkTypes = Seq(INTTYPE, LONGTYPE, STRINGTYPE, AUTOTYPE)

        if (fields.count(_.pk) > 1) {
          return Failure(EntityNotProperlyDefinedException(Some("Entity defined with more than one primary key")))
        } else if (fields.filter(_.pk).isEmpty) {
          return Failure(EntityNotProperlyDefinedException(Some("Entity defined has no primary key.")))
        } else if (!fields.filter(_.pk).forall(field => allowedPkTypes.contains(field.fieldtype))) {
          return Failure(EntityNotProperlyDefinedException(Some("Entity defined needs a " + allowedPkTypes.map(_.name).mkString(", ") + " primary key")))
        }

        if (fields.count(_.fieldtype == AUTOTYPE) > 1) {
          log.error("only one auto type allowed, and only in primary key")
          return Failure(EntityNotProperlyDefinedException(Some("Too many auto fields defined.")))
        } else if (fields.count(_.fieldtype == AUTOTYPE) > 0 && !fields.filter(_.pk).forall(_.fieldtype == AUTOTYPE)) {
          return Failure(EntityNotProperlyDefinedException(Some("Auto type only allowed in primary key.")))
        }


        val pk = fields.filter(_.pk).head
        val featureFields = fields.filter(_.fieldtype == FEATURETYPE)

        //perform
        featureStorage.create(entityname, featureFields.+:(pk))

        if (!fields.filterNot(_.fieldtype == FEATURETYPE).filterNot(_.pk).isEmpty) {
          val metadataFields = fields.filterNot(_.fieldtype == FEATURETYPE)
          metadataStorage.create(entityname, metadataFields)
          CatalogOperator.createEntity(entityname, pk.name, fields, true)
          Success(Entity(entityname, pk.name, featureStorage, Option(metadataStorage)))
        } else {
          CatalogOperator.createEntity(entityname, pk.name, fields, false)
          Success(Entity(entityname, pk.name, featureStorage, None))
        }
      }
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Drops an entity.
    *
    * @param entityname
    * @param ifExists
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    lock.synchronized {
      if (!exists(entityname)) {
        if (!ifExists) {
          return Failure(EntityNotExistingException())
        } else {
          return Success(null)
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
  }

  /**
    * Inserts data into an entity.
    *
    * @param entityname
    * @param insertion data frame containing all columns (of both the feature storage and the metadata storage);
    *                  note that you should name the feature column as ("feature").
    * @param ignoreChecks
    * @return
    */
  def insertData(entityname: EntityName, insertion: DataFrame, ignoreChecks: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    val entity = load(entityname).get

    entity.insert(insertion, ignoreChecks)
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

    val pk = CatalogOperator.getEntityPK(entityname)

    Success(Entity(entityname, pk, featureStorage, entityMetadataStorage))
  }


  /**
    * Returns number of tuples in entity (only feature storage is considered).
    *
    * @param entityname
    * @return the number of tuples in the entity
    */
  def countTuples(entityname: EntityName)(implicit ac: AdamContext): Try[Long] = {
    val entity = load(entityname)
    if (entity.isSuccess) {
      entity.get.count
    } else {
      Failure(entity.failed.get)
    }
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
      Success(entity.get.properties)
    } else {
      Failure(entity.failed.get)
    }
  }
}
