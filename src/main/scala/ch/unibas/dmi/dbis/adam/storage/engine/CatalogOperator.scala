package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{EntityNameHolder, AttributeDefinition}
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, IndexExistingException, IndexNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.query.scanweight.ScanWeightHandler
import org.apache.commons.io.FileUtils
import ch.unibas.dmi.dbis.adam.utils.Logging
import slick.driver.PostgresDriver.api._
import slick.jdbc.meta.MTable

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Success, Failure, Try}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object CatalogOperator extends Logging {
  private val MAX_WAITING_TIME: Duration = 100.seconds

  private val db = Database.forURL(AdamConfig.jdbcUrl, driver = "org.postgresql.Driver", user = AdamConfig.jdbcUser, password = AdamConfig.jdbcPassword)

  //generate catalog entities in the beginning if not already existent
  val entityList = Await.result(db.run(MTable.getTables), MAX_WAITING_TIME).toList.map(x => x.name.name)
  Catalog().filterNot(mdd => entityList.contains(mdd._1)).foreach(mdd => {
    db.run(mdd._2.schema.create)
  })

  private val entities = TableQuery[EntitiesCatalog]
  private val entityfields = TableQuery[EntityFieldsCatalog]
  private val indexes = TableQuery[IndexesCatalog]

  private val DEFAULT_DIMENSIONALITY: Int = -1
  private val DEFAULT_WEIGHT: Float = ScanWeightHandler.DEFAULT_WEIGHT


  /**
    *
    * @param entityname   name of entity
    * @param featurepath  path of feature data
    * @param metadatapath path of metadata data
    * @param attributes   attributes
    * @param withMetadata has metadata
    * @return
    */
  def createEntity(entityname: EntityName, featurepath: Option[String], metadatapath: Option[String], attributes: Seq[AttributeDefinition], withMetadata: Boolean = false): Boolean = {
    if (existsEntity(entityname)) {
      throw new EntityExistingException()
    }

    val setup = DBIO.seq(
      entities.+=(entityname, featurepath.getOrElse(""), metadatapath.getOrElse(""), withMetadata)
    )

    Await.result(db.run(setup), MAX_WAITING_TIME)

    attributes.foreach { field =>
      val setup = DBIO.seq(entityfields.+=(field.name, field.fieldtype.name, field.pk, field.unique, field.indexed, entityname, DEFAULT_DIMENSIONALITY, DEFAULT_WEIGHT))
      Await.result(db.run(setup), MAX_WAITING_TIME)
    }

    log.debug("created entity in catalog")
    true
  }

  /**
    *
    * @param entityname name of entity
    * @return
    */
  def getEntityFeaturePath(entityname: EntityName): String = {
    val query = entities.filter(_.entityname === entityname.toString()).map(_.featurepath).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param entityname name of entity
    * @return
    */
  def getEntityMetadataPath(entityname: EntityName): String = {
    val query = entities.filter(_.entityname === entityname.toString()).map(_.metadatapath).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param entityname name of entity
    * @param newPath    new path
    * @return
    */
  def updateEntityFeaturePath(entityname: EntityName, newPath: String): Boolean = {
    val query = entities.filter(_.entityname === entityname.toString()).map(_.featurepath)

    val update = DBIO.seq(
      query.update(newPath)
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("updated entity path in catalog")
    true
  }

  /**
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @return
    */
  def getEntityWeight(entityname: EntityName, attribute: String): Float = {
    val query = entityfields.filter(_.entityname === entityname.toString).filter(_.fieldname === attribute).map(_.scanweight).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param newWeight  specify the new weight for the entity (the higher the more important), if no weight is
    *                   specified the default weight is used
    * @return
    */
  def updateEntityWeight(entityname: EntityName, attribute: String, newWeight: Option[Float] = Some(DEFAULT_WEIGHT)): Boolean = {
    val query = entityfields.filter(_.entityname === entityname.toString).filter(_.fieldname === attribute).map(_.scanweight)

    val update = DBIO.seq(
      query.update(newWeight.getOrElse(DEFAULT_WEIGHT))
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("updated weight in catalog")
    true
  }


  /**
    * Drops entity from catalog.
    *
    * @param entityname name of entity
    * @param ifExists   no error if does not exist
    */
  def dropEntity(entityname: EntityName, ifExists: Boolean = false): Boolean = {
    if (!existsEntity(entityname)) {
      if (!ifExists) {
        throw new EntityNotExistingException()
      } else {
        return false
      }
    }

    Await.result(db.run(entityfields.filter(_.entityname === entityname.toString()).delete), MAX_WAITING_TIME)
    Await.result(db.run(entities.filter(_.entityname === entityname.toString()).delete), MAX_WAITING_TIME)

    log.debug("dropped entity from catalog")

    true
  }

  /**
    * Checks whether entity exists in catalog.
    *
    * @param entityname name of entity
    * @return
    */
  def existsEntity(entityname: EntityName): Boolean = {
    val query = entities.filter(_.entityname === entityname.toString()).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    count > 0
  }

  /**
    *
    * @param entityname name of entity
    * @return
    */
  def getEntityPK(entityname: EntityName): AttributeDefinition = {
    val query = entityfields.filter(_.entityname === entityname.toString()).filter(_.pk).result
    val fields = Await.result(db.run(query), MAX_WAITING_TIME)

    fields.map(x => AttributeDefinition(x._1, FieldTypes.fromString(x._2), x._3, x._4, x._5)).head
  }

  /**
    *
    * @param entityname name of entity
    * @return
    */
  def getFields(entityname: EntityName): Seq[AttributeDefinition] = {
    val query = entityfields.filter(_.entityname === entityname.toString()).result
    val fields = Await.result(db.run(query), MAX_WAITING_TIME)

    fields.map(x => AttributeDefinition(x._1, FieldTypes.fromString(x._2), x._3, x._4, x._5))
  }

  /**
    * Checks whether entity has metadata.
    *
    * @param entityname name of entity
    * @return
    */
  def hasEntityMetadata(entityname: EntityName): Boolean = {
    val query = entities.filter(_.entityname === entityname.toString()).map(_.hasMeta).take(1).result
    Await.result(db.run(query), MAX_WAITING_TIME).head
  }

  /**
    * Lists all entities in catalog.
    *
    * @return name of entities
    */
  def listEntities(): Seq[EntityName] = {
    val query = entities.map(_.entityname).result
    Await.result(db.run(query), MAX_WAITING_TIME).map(EntityNameHolder(_))
  }

  /**
    * Checks whether index exists in catalog.
    *
    * @param indexname name of index
    * @return
    */
  def existsIndex(indexname: IndexName): Boolean = {
    val query = indexes.filter(_.indexname === indexname).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    count > 0
  }

  /**
    * Creates index in catalog.
    *
    * @param indexname  name of index
    * @param entityname name of entity
    * @param indexmeta
    */
  def createIndex(indexname: IndexName, path: String, entityname: EntityName, column: String, indextypename: IndexTypeName, indexmeta: Serializable): Boolean = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    if (existsIndex(indexname)) {
      throw new IndexExistingException()
    }

    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(indexmeta)
    val data = bos.toByteArray
    oos.close()
    bos.close()

    val setup = DBIO.seq(
      indexes.+=((indexname, entityname, column, indextypename.name, path, data, true, DEFAULT_WEIGHT))
    )

    Await.result(db.run(setup), MAX_WAITING_TIME)
    log.debug("created index in catalog")

    true
  }

  /**
    * Drops index from catalog.
    *
    * @param indexname name of index
    * @return
    */
  def dropIndex(indexname: IndexName): Boolean = {
    if (!existsIndex(indexname)) {
      throw new IndexNotExistingException()
    }
    
    val query = indexes.filter(_.indexname === indexname).delete
    Await.result(db.run(query), MAX_WAITING_TIME)
    log.debug("dropped index from catalog")

    true
  }

  /**
    * Drops all indexes from catalog belonging to entity.
    *
    * @param entityname name of entity
    * @return names of indexes dropped
    */
  def dropAllIndexes(entityname: EntityName): Seq[IndexName] = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    val existingIndexes = listIndexes(entityname).map(_._1)

    val query = indexes.filter(_.entityname === entityname.toString()).delete
    Await.result(db.run(query), MAX_WAITING_TIME)

    existingIndexes
  }

  /**
    * Lists all indexes in catalog.
    *
    * @param entityname    filter by entityname, set to null for not using filter
    * @param indextypename filter by indextypename, set to null for not using filter
    * @return
    */
  def listIndexes(entityname: EntityName = null, indextypename: IndexTypeName = null): Seq[(IndexName, IndexTypeName, Float)] = {
    var catalog: Query[IndexesCatalog, (String, String, String, String, String, Array[Byte], Boolean, Float), Seq] = indexes

    if (entityname != null) {
      catalog = catalog.filter(_.entityname === entityname.toString())
    }

    if (indextypename != null) {
      catalog = catalog.filter(_.indextypename === indextypename.name)
    }

    val query = catalog.map(index => (index.indexname, index.indextypename, index.scanweight)).result
    Await.result(db.run(query), MAX_WAITING_TIME).map(index => (index._1, IndexTypes.withName(index._2).get, index._3))
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def getIndexColumn(indexname: IndexName): String = {
    val query = indexes.filter(_.indexname === indexname).map(_.fieldname).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    * Returns meta information to a specified index.
    *
    * @param indexname name of index
    * @return
    */
  def getIndexMeta(indexname: IndexName): Try[Any] = {
    try {
      val query = indexes.filter(_.indexname === indexname).map(_.indexmeta).result.head
      val data = Await.result(db.run(query), MAX_WAITING_TIME)

      val bis = new ByteArrayInputStream(data)
      val ois = new ObjectInputStream(bis)
      Success(ois.readObject())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def getIndexPath(indexname: IndexName): String = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexpath).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param indexname name of index
    * @param newPath   new path for index
    * @return
    */
  def updateIndexPath(indexname: IndexName, newPath: String): Boolean = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexpath)

    val update = DBIO.seq(
      query.update(newPath)
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("updated index path in catalog")
    true
  }

  /**
    * Returns type name of index
    *
    * @param indexname name of index
    * @return
    */
  def getIndexTypeName(indexname: IndexName): IndexTypeName = {
    val query = indexes.filter(_.indexname === indexname).map(_.indextypename).result.head
    val result = Await.result(db.run(query), MAX_WAITING_TIME)

    IndexTypes.withName(result).get
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def getIndexWeight(indexname: IndexName): Float = {
    val query = indexes.filter(_.indexname === indexname).map(_.scanweight).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param indexname name of index
    * @param newWeight specify the new weight for the index (the higher the more important), if no weight is
    *                  specified the default weight is used
    * @return
    */
  def updateIndexWeight(indexname: IndexName, newWeight: Option[Float] = Some(DEFAULT_WEIGHT)): Boolean = {
    val query = indexes.filter(_.indexname === indexname).map(_.scanweight)

    val update = DBIO.seq(
      query.update(newWeight.getOrElse(DEFAULT_WEIGHT))
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("updated weight in catalog")
    true
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def isIndexUptodate(indexname: IndexName): Boolean = {
    val query = indexes.filter(_.indexname === indexname).map(_.uptodate).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    * Mark indexes for given entity stale.
    *
    * @param entityname name of entity
    */
  def updateIndexesToStale(entityname: EntityName): Boolean = {
    val query = indexes.filter(_.entityname === entityname.toString()).map(_.uptodate)

    val update = DBIO.seq(
      query.update(false) //TODO: possibly also change weight
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("made indexes stale in catalog")
    true
  }

  /**
    * Returns the name of the entity corresponding to the index name
    *
    * @param indexname name of index
    * @return
    */
  def getEntitynameFromIndex(indexname: IndexName): EntityName = {
    val query = indexes.filter(_.indexname === indexname).map(_.entityname).result.head
    val name = Await.result(db.run(query), MAX_WAITING_TIME)
    EntityNameHolder(name)
  }
}
