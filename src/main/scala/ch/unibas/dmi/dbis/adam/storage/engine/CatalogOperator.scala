package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{EntityNameHolder, FieldDefinition}
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, IndexExistingException, IndexNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import org.apache.commons.io.FileUtils
import org.apache.spark.Logging
import slick.driver.H2Driver.api._
import slick.jdbc.meta.MTable

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object CatalogOperator extends Logging {
  private val MAX_WAITING_TIME: Duration = 100.seconds

  private val db = Database.forURL("jdbc:h2:" + (AdamConfig.catalogPath + "/" + "catalog"), driver = "org.h2.Driver")

  //generate catalog entities in the beginning if not already existent
  val entityList = Await.result(db.run(MTable.getTables), MAX_WAITING_TIME).toList.map(x => x.name.name)
  Catalog().filterNot(mdd => entityList.contains(mdd._1)).foreach(mdd => {
    db.run(mdd._2.schema.create)
  })

  private val entities = TableQuery[EntitiesCatalog]
  private val entityfields = TableQuery[EntityFieldsCatalog]
  private val indexes = TableQuery[IndexesCatalog]

  private val DEFAULT_DIMENSIONALITY : Int = -1
  private val DEFAULT_INDEX_WEIGHT : Float = 100


  /**
    * Creates entity in catalog.
    *
    * @param entityname
    * @param withMetadata
    * @return
    */
  def createEntity(entityname: EntityName, featurepath : Option[String], metadatapath : Option[String], fields: Seq[FieldDefinition], withMetadata: Boolean = false): Boolean = {
    if (existsEntity(entityname)) {
      throw new EntityExistingException()
    }

    val setup = DBIO.seq(
      entities.+=(entityname, featurepath.getOrElse(""), metadatapath.getOrElse(""), withMetadata)
    )

    Await.result(db.run(setup), MAX_WAITING_TIME)

    fields.foreach { field =>
      val setup = DBIO.seq(entityfields.+=(field.name, field.fieldtype.name, field.pk, field.unique, field.indexed, entityname, DEFAULT_DIMENSIONALITY))
      Await.result(db.run(setup), MAX_WAITING_TIME)
    }

    log.debug("created entity in catalog")
    true
  }

  /**
    *
    * @param entityname
    * @return
    */
  def getEntityFeaturePath(entityname: EntityName): String = {
    val query = entities.filter(_.entityname === entityname.toString()).map(_.featurepath).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param entityname
    * @return
    */
  def getEntityMetadataPath(entityname: EntityName): String = {
    val query = entities.filter(_.entityname === entityname.toString()).map(_.metadatapath).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param entityname
    * @param newPath
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

    Await.result(db.run(entityfields.filter(_.entityname === entityname.toString()).delete), MAX_WAITING_TIME)
    Await.result(db.run(entities.filter(_.entityname === entityname.toString()).delete), MAX_WAITING_TIME)

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
    val query = entities.filter(_.entityname === entityname.toString()).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    (count > 0)
  }

  /**
    *
    * @param entityname
    * @return
    */
  def getEntityPK(entityname: EntityName) : FieldDefinition = {
    val query = entityfields.filter(_.entityname === entityname.toString()).filter(_.pk).result
    val fields = Await.result(db.run(query), MAX_WAITING_TIME)

    fields.map(x => FieldDefinition(x._1, FieldTypes.fromString(x._2), x._3, x._4, x._5)).head
  }

  /**
    *
    * @param entityname
    * @return
    */
  def getFields(entityname : EntityName) : Seq[FieldDefinition] = {
    val query = entityfields.filter(_.entityname === entityname.toString()).result
    val fields = Await.result(db.run(query), MAX_WAITING_TIME)

    fields.map(x => FieldDefinition(x._1, FieldTypes.fromString(x._2), x._3, x._4, x._5))
  }

  /**
    * Checks whether entity has metadata.
    *
    * @param entityname
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
  def createIndex(indexname: IndexName, path: String, entityname: EntityName, column: String, indextypename: IndexTypeName, indexmeta: Serializable): Boolean = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    if (existsIndex(indexname)) {
      throw new IndexExistingException()
    }

    val metaPath = AdamConfig.indexMetaCatalogPath + "/" + indexname + "/" //TODO: change this
    val metaFilePath = metaPath + "_adam_metadata"

    new File(metaPath).mkdirs()

    val oos = new ObjectOutputStream(new FileOutputStream(metaFilePath))
    oos.writeObject(indexmeta)
    oos.close

    val setup = DBIO.seq(
      indexes.+=((indexname, entityname, column, indextypename.name, path, metaFilePath, true, DEFAULT_INDEX_WEIGHT))
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

    val metaPath = AdamConfig.indexMetaCatalogPath + "/" + indexname + "/" //TODO: change this
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
    var catalog: Query[IndexesCatalog, (String, String, String, String, String, String, Boolean, Float), Seq] = indexes

    if (entityname != null) {
      catalog = catalog.filter(_.entityname === entityname.toString())
    }

    if (indextypename != null) {
      catalog = catalog.filter(_.indextypename === indextypename.name)
    }

    val query = catalog.map(index => (index.indexname, index.indextypename, index.indexweight)).result
    Await.result(db.run(query), MAX_WAITING_TIME).map(index => (index._1, IndexTypes.withName(index._2).get, index._3))
  }

  /**
    *
    * @param indexname
    * @return
    */
  def getIndexColumn(indexname: IndexName): String = {
    val query = indexes.filter(_.indexname === indexname).map(_.fieldname).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    * Returns meta information to a specified index.
    *
    * @param indexname
    * @return
    */
  def getIndexMeta(indexname: IndexName): Any = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexmetapath).result.head
    val path = Await.result(db.run(query), MAX_WAITING_TIME)
    val ois = new ObjectInputStream(new FileInputStream(path))
    ois.readObject()
  }

  /**
    *
    * @param indexname
    * @return
    */
  def getIndexPath(indexname: IndexName): String = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexpath).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param indexname
    * @param newPath
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
    * @param indexname
    * @return
    */
  def getIndexTypeName(indexname: IndexName): IndexTypeName = {
    val query = indexes.filter(_.indexname === indexname).map(_.indextypename).result.head
    val result = Await.result(db.run(query), MAX_WAITING_TIME)

    IndexTypes.withName(result).get
  }

  /**
    *
    * @param indexname
    * @return
    */
  def getIndexWeight(indexname: IndexName): Float = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexweight).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    *
    * @param indexname
    * @param newWeight specify the new weight for the index (the higher the more important), if no weight is
    *                  specified the default index weight is used
    * @return
    */
  def updateIndexWeight(indexname: IndexName, newWeight: Float = DEFAULT_INDEX_WEIGHT): Boolean = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexweight)

    val update = DBIO.seq(
      query.update(newWeight)
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("updated weight in catalog")
    true
  }

  /**
    *
    * @param entityname
    * @return
    */
  def resetIndexWeight(entityname: EntityName): Boolean = {
    val query = indexes.filter(_.entityname === entityname.toString()).map(_.indexweight)

    val update = DBIO.seq(
      query.update(DEFAULT_INDEX_WEIGHT)
    )
    Await.result(db.run(update), MAX_WAITING_TIME)

    log.debug("updated weight in catalog")
    true
  }

  /**
    *
    * @param indexname
    * @return
    */
  def isIndexUptodate(indexname: IndexName): Boolean = {
    val query = indexes.filter(_.indexname === indexname).map(_.uptodate).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
    * Mark indexes for given entity stale.
 *
    * @param entityname
    */
  def updateIndexesToStale(entityname : EntityName) : Boolean = {
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
    * @param indexname
    * @return
    */
  def getEntitynameFromIndex(indexname: IndexName): EntityName = {
    val query = indexes.filter(_.indexname === indexname).map(_.entityname).result.head
    val name = Await.result(db.run(query), MAX_WAITING_TIME)
    EntityNameHolder(name)
  }
}
