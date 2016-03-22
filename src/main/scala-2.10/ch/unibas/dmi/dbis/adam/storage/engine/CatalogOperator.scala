package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, IndexExistingException, IndexNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import slick.dbio.Effect.Read
import slick.driver.H2Driver.api._
import slick.jdbc.meta.MTable
import slick.profile.SqlAction

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object CatalogOperator {
  val log = Logger.getLogger(getClass.getName)

  private val MAX_WAITING_TIME: Duration = 100.seconds

  private val db = Database.forURL("jdbc:h2:" + (AdamConfig.catalogPath + "/" + "catalog"), driver = "org.h2.Driver")

  //generate catalog entities in the beginning if not already existent
  val entityList = Await.result(db.run(MTable.getTables), MAX_WAITING_TIME).toList.map(x => x.name.name)
  Catalog().filterNot(mdd => entityList.contains(mdd._1)).foreach(mdd => {
    db.run(mdd._2.schema.create)
  })


  private val entities = TableQuery[EntitiesCatalog]
  private val indexes = TableQuery[IndexesCatalog]


  /**
    * Creates entity in catalog.
    *
    * @param entityname
    * @param withMetadata
    * @return
    */
  def createEntity(entityname: EntityName, withMetadata : Boolean = false): Boolean = {
    if (existsEntity(entityname)) {
      throw new EntityExistingException()
    }

    val setup = DBIO.seq(
      entities.+=(entityname, withMetadata)
    )

    Await.result(db.run(setup), MAX_WAITING_TIME)

    log.debug("created entity in catalog")
    true
  }

  /**
    * Drops entity from catalog.
    *
    * @param entityname
    * @param ifExists
    */
  def dropEntity(entityname: EntityName, ifExists: Boolean = false): Boolean = {
    if (!existsEntity(entityname)) {
      if (!ifExists) {
        throw new EntityNotExistingException()
      } else {
        return false
      }
    }

    val query = entities.filter(_.entityname === entityname).delete
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    log.debug("dropped entity from catalog")

    true
  }

  /**
    * Checks whether entity exists in catalog.
    *
    * @param entityname
    * @return
    */
  def existsEntity(entityname: EntityName): Boolean = {
    val query = entities.filter(_.entityname === entityname).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    (count > 0)
  }

  /**
    * Checks whether entity has metadata.
    *
    * @param entityname
    * @return
    */
  def hasEntityMetadata(entityname : EntityName) : Boolean = {
    val query = entities.filter(_.entityname === entityname).map(_.hasMeta).take(1).result
    Await.result(db.run(query), MAX_WAITING_TIME).head
  }

  /**
    * Lists all entities in catalog.
    *
    * @return name of entities
    */
  def listEntities(): List[EntityName] = {
    val query = entities.map(_.entityname).result
    Await.result(db.run(query), MAX_WAITING_TIME).toList
  }

  /**
    * Checks whether index exists in catalog.
    *
    * @param indexname
    * @return
    */
  def existsIndex(indexname: IndexName): Boolean = {
    val query = indexes.filter(_.indexname === indexname).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    (count > 0)
  }

  /**
    * Creates index in catalog.
    *
    * @param indexname
    * @param entityname
    * @param indexmeta
    */
  def createIndex(indexname: IndexName, entityname: EntityName, indextypename: IndexTypeName, indexmeta: Serializable): Boolean = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    if (existsIndex(indexname)) {
      throw new IndexExistingException()
    }

    val metaPath = AdamConfig.indexMetaCatalogPath + "/" + indexname + "/"
    val metaFilePath = metaPath + "_adam_metadata"

    new File(metaPath).mkdirs()

    val oos = new ObjectOutputStream(new FileOutputStream(metaFilePath))
    oos.writeObject(indexmeta)
    oos.close

    val setup = DBIO.seq(
      indexes.+=((indexname, entityname, indextypename.toString, metaFilePath))
    )

    Await.result(db.run(setup), MAX_WAITING_TIME)
    log.debug("created index in catalog")

    true
  }

  /**
    * Drops index from catalog.
    *
    * @param indexname
    * @return
    */
  def dropIndex(indexname: IndexName): Boolean = {
    if (!existsIndex(indexname)) {
      throw new IndexNotExistingException()
    }

    val metaPath = AdamConfig.indexMetaCatalogPath + "/" + indexname + "/"
    FileUtils.deleteDirectory(new File(metaPath))

    val query = indexes.filter(_.indexname === indexname).delete
    Await.result(db.run(query), MAX_WAITING_TIME)
    log.debug("dropped index from catalog")

    true
  }

  /**
    * Drops all indexes from catalog belonging to entity.
    *
    * @param entityname
    * @return names of indexes dropped
    */
  def dropAllIndexes(entityname: EntityName) : Seq[IndexName] = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    val existingIndexes = listIndexes(entityname)

    val query = indexes.filter(_.entityname === entityname).delete
    Await.result(db.run(query), MAX_WAITING_TIME)

    existingIndexes
  }

  /**
    * Lists all indexes in catalog for specific entity.
    *
    * @param entityname
    */
  def listIndexes(entityname: EntityName): Seq[IndexName] = {
    val query = indexes.filter(_.entityname === entityname).map(_.indexname).result
    Await.result(db.run(query), MAX_WAITING_TIME).toList
  }


  /**
    * Lists all indexes in catalog together with type information for a specific entity.
    *
    * @param entityname
    */
  def listIndexesWithType(entityname: EntityName): Seq[(IndexName, IndexTypeName)] = {
    val query = indexes.filter(_.entityname === entityname).map(index => (index.indexname, index.indextypename)).result
    Await.result(db.run(query), MAX_WAITING_TIME).map(index => (index._1, IndexTypes.withName(index._2).get))
  }

  /**
    * Lists all indexes.
    *
    * @return
    */
  def listIndexes(): Seq[IndexName] = {
    val query = indexes.map(_.indexname).result
    Await.result(db.run(query), MAX_WAITING_TIME).toList
  }


  /**
    * Returns meta information to a specified index.
    *
    * @param indexname
    * @return
    */
  def getIndexMeta(indexname: IndexName): Any = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexmeta).result.head
    val path = Await.result(db.run(query), MAX_WAITING_TIME)
    val ois = new ObjectInputStream(new FileInputStream(path))
    ois.readObject()
  }

  /**
    * Returns type name of index
    *
    * @param indexname
    * @return
    */
  def getIndexTypeName(indexname: IndexName): IndexTypeName = {
    val query: SqlAction[String, NoStream, Read] = indexes.filter(_.indexname === indexname).map(_.indextypename).result.head
    val result = Await.result(db.run(query), MAX_WAITING_TIME)

    IndexTypes.withName(result).get
  }

  /**
    * Returns the name of the entity corresponding to the index name
    *
    * @param indexname
    * @return
    */
  def getEntitynameFromIndex(indexname: IndexName): EntityName = {
    val query = indexes.filter(_.indexname === indexname).map(_.entityname).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }
}
