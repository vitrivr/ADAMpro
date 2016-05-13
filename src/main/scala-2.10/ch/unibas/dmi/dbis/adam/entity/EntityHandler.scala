package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldTypes._
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, EntityNotProperlyDefinedException}
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.util.{Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object EntityHandler extends Logging {
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
          CatalogOperator.createEntity(entityname, fields, true)
          Success(Entity(entityname, featureStorage, Option(metadataStorage)))
        } else {
          CatalogOperator.createEntity(entityname, fields, false)
          Success(Entity(entityname, featureStorage, None))
        }
      }
    } catch {
      case e: Exception => {
        //TODO: possibly drop what has been already created
        Failure(e)
      }
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
      entity.get.featureData.rdd.setName(entityname + "_feature").cache()

      val meta = entity.get.metaData

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

    Success(Entity(entityname, featureStorage, entityMetadataStorage))
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list(): Seq[EntityName] = CatalogOperator.listEntities()
}
