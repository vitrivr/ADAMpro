package ch.unibas.dmi.dbis.adam.catalog

import java.io._

import ch.unibas.dmi.dbis.adam.catalog.catalogs._
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, IndexExistingException, IndexNotExistingException}
import ch.unibas.dmi.dbis.adam.helpers.scanweight.ScanWeightBenchmarker
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.utils.Logging
import slick.dbio.NoStream
import slick.driver.PostgresDriver.api._
import slick.jdbc.meta.MTable

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
object CatalogOperator extends Logging {
  def apply() = List(
    ("entity", TableQuery[EntityCatalog]),
    ("entityoptions", TableQuery[EntityOptionsCatalog]),
    ("attribute", TableQuery[AttributeCatalog]),
    ("attributeoptions", TableQuery[AttributeOptionsCatalog]),
    ("attributeweight", TableQuery[AttributeWeightCatalog]),
    ("index", TableQuery[IndexCatalog]),
    ("indexoptions", TableQuery[IndexOptionsCatalog]),
    ("indexweight", TableQuery[IndexWeightCatalog])
  )

  private val MAX_WAITING_TIME: Duration = 100.seconds
  private val DB = Database.forURL(AdamConfig.jdbcUrl, driver = "org.postgresql.Driver", user = AdamConfig.jdbcUser, password = AdamConfig.jdbcPassword)
  private[catalog] val SCHEMA = "adampro"

  //generate catalog tables in the beginning if not already existent
  private val metas = Await.result(DB.run(MTable.getTables), MAX_WAITING_TIME).toList.map(x => x.name.name).filter(apply().map(_._1).contains(_))
  if (metas.isEmpty) {
    //schema might be empty
    Await.result(DB.run(sqlu"""create schema if not exists adampro;"""), MAX_WAITING_TIME)
  }
  apply().filterNot(mdd => metas.contains(mdd._1)).foreach(mdd => {
    DB.run(mdd._2.schema.create)
  })

  private val _entitites = TableQuery[EntityCatalog]
  private val _entityOptions = TableQuery[EntityOptionsCatalog]
  private val _attributes = TableQuery[AttributeCatalog]
  private val _attributeOptions = TableQuery[AttributeOptionsCatalog]
  private val _attributeweight = TableQuery[AttributeWeightCatalog]
  private val _indexes = TableQuery[IndexCatalog]
  private val _indexOptions = TableQuery[IndexOptionsCatalog]
  private val _indexweights = TableQuery[IndexWeightCatalog]

  private val DEFAULT_WEIGHT: Float = ScanWeightBenchmarker.DEFAULT_WEIGHT


  /**
    *
    * @param entityname name of entity
    * @param attributes attributes
    * @return
    */
  def createEntity(entityname: EntityName, attributes: Seq[AttributeDefinition]): Boolean = {
    try {
      if (existsEntity(entityname)) {
        throw new EntityExistingException()
      }

      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      actions += DBIO.seq(_entitites.+=(entityname))

      attributes.foreach { attribute =>
        actions += _attributes.+=(entityname, attribute.name, attribute.fieldtype.name, attribute.pk, attribute.unique, attribute.indexed, attribute.storagehandler.map(_.name).getOrElse(""))

        attribute.params.foreach { case (key, value) =>
          actions += _attributeOptions.+=(entityname, attribute.name, key, value)
        }

        actions += _attributeweight.+=(entityname, attribute.name, DEFAULT_WEIGHT)
      }

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

      log.debug("created entity in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }


  /**
    * Returns the metadata to an entity.
    *
    * @param entityname name of entity
    * @param key        name of key
    * @return
    */
  def getEntityOption(entityname: EntityName, key: String): Option[String] = {
    try {
      val result = Await.result(DB.run(_entityOptions.filter(_.entityname === entityname.toString).map(_.value).result.head), MAX_WAITING_TIME)

      log.debug("got entity option from catalog")
      if (result == null || result.length == 0) {
        None
      } else {
        Some(result)
      }
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        None
    }
  }


  /**
    * Updates or inserts a metadata information to an entity.
    *
    * @param entityname name of entity
    * @param key        name of key
    * @param newValue   new value
    * @return
    */
  def updateEntityOption(entityname: EntityName, key: String, newValue: String): Boolean = {
    try {
      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      //upsert
      actions += _entityOptions.filter(_.entityname === entityname.toString()).filter(_.key === key).delete
      actions += _entityOptions.+=(entityname, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

      log.debug("updated entity option in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Deletes the metadata to an entity.
    *
    * @param entityname name of entity
    * @param key        name of key
    * @return
    */
  def deleteEntityOption(entityname: EntityName, key: String): Boolean = {
    try {
      Await.result(DB.run(_entityOptions.filter(_.entityname === entityname.toString).delete), MAX_WAITING_TIME)

      log.debug("delete entity option from catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }


  /**
    * Gets the metadata to an attribute of an entity.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param key        name of key
    * @return
    */
  def getAttributeOption(entityname: EntityName, attribute: String, key: String): Option[String] = {
    try {
      val result = Await.result(DB.run(_attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).map(_.value).result.head), MAX_WAITING_TIME)


      log.debug("got attribute option from catalog")
      if (result == null || result.length == 0) {
        None
      } else {
        Some(result)
      }
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        None
    }
  }


  /**
    * Updates or inserts a metadata information to an attribute.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param key        name of key
    * @param newValue   new value
    * @return
    */
  def updateAttributeOption(entityname: EntityName, attribute: String, key: String, newValue: String): Boolean = {
    try {
      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      //upsert
      actions += _attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).filter(_.key === key).delete
      actions += _attributeOptions.+=(entityname, attribute, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

      log.debug("updated attribute option in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Deletes the metadata to an attribute.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param key        name of key
    * @return
    */
  def deleteAttributeOption(entityname: EntityName, attribute: String, key: String): Boolean = {
    try {
      Await.result(DB.run(_attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).delete), MAX_WAITING_TIME)

      log.debug("delete attribute option from catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Gets the metadata to an index.
    *
    * @param indexname name of index
    * @param key       name of key
    * @return
    */
  def getIndexOption(indexname: IndexName, key: String): Option[String] = {
    try {
      val result = Await.result(DB.run(_indexOptions.filter(_.indexname === indexname.toString).map(_.value).result.head), MAX_WAITING_TIME)

      log.debug("got index option from catalog")
      if (result == null || result.length == 0) {
        None
      } else {
        Some(result)
      }
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        None
    }
  }


  /**
    * Updates or inserts a metadata information to an index.
    *
    * @param indexname name of index
    * @param key       name of key
    * @param newValue  new value
    * @return
    */
  def updateIndexOption(indexname: IndexName, key: String, newValue: String): Boolean = {
    try {
      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      //upsert
      actions += _indexOptions.filter(_.indexname === indexname.toString).filter(_.key === key).delete
      actions += _indexOptions.+=(indexname.toString, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

      log.debug("updated index option in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Deletes the metadata to an index.
    *
    * @param indexname name of index
    * @param key       name of key
    * @return
    */
  def deleteIndexOption(indexname: IndexName, key: String): Boolean = {
    try {
      Await.result(DB.run(_indexOptions.filter(_.indexname === indexname.toString).delete), MAX_WAITING_TIME)

      log.debug("delete index option from catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }


  /**
    * Returns the scan weight to an attribute of an entity.
    *
    * @param entityname    name of entity
    * @param attributename name of attribute
    * @return
    */

  def getEntityScanWeight(entityname: EntityName, attributename: String): Float = {
    Await.result(DB.run(_attributeweight.filter(_.entityname === entityname.toString).filter(_.attributename === attributename).map(_.weight).result.head), MAX_WAITING_TIME)
  }

  /**
    * Sets the scan weight to an attribute of an entity.
    *
    * @param entityname    name of entity
    * @param attributename name of attribute
    * @param newWeight     specify the new weight for the entity (the higher the more important), if no weight is
    *                      specified the default weight is used
    * @return
    */
  def setEntityScanWeight(entityname: EntityName, attributename: String, newWeight: Option[Float] = Some(DEFAULT_WEIGHT)): Boolean = {
    try {
      val update = _attributeweight.filter(_.entityname === entityname.toString).filter(_.attributename === attributename).map(_.weight).update(newWeight.getOrElse(DEFAULT_WEIGHT))
      Await.result(DB.run(update), MAX_WAITING_TIME)

      log.debug("updated weight in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Gets the scan weight of an index.
    *
    * @param indexname name of index
    * @return
    */
  def getIndexScanWeight(indexname: IndexName): Float = {
    Await.result(DB.run(_indexweights.filter(_.indexname === indexname.toString).map(_.weight).result.head), MAX_WAITING_TIME)
  }

  /**
    * Sets the scan weight of an index.
    *
    * @param indexname name of index
    * @param newWeight specify the new weight for the entity (the higher the more important), if no weight is
    *                  specified the default weight is used
    * @return
    */
  def setIndexScanWeight(indexname: IndexName, newWeight: Option[Float] = Some(DEFAULT_WEIGHT)): Boolean = {
    try {
      val update = _indexweights.filter(_.indexname === indexname.toString).map(_.weight).update(newWeight.getOrElse(DEFAULT_WEIGHT))
      Await.result(DB.run(update), MAX_WAITING_TIME)

      log.debug("updated weight in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
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

    try {
      //on delete cascade...
      Await.result(DB.run(_entitites.filter(_.entityname === entityname.toString()).delete), MAX_WAITING_TIME)

      log.debug("dropped entity from catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Checks whether entity exists in catalog.
    *
    * @param entityname name of entity
    * @return
    */
  def existsEntity(entityname: EntityName): Boolean = {
    Await.result(DB.run(_entitites.filter(_.entityname === entityname.toString()).length.result), MAX_WAITING_TIME) > 0
  }

  /**
    *
    * @param entityname name of entity
    * @return
    */
  def getEntityPK(entityname: EntityName): AttributeDefinition = {
    val query = _attributes.filter(_.entityname === entityname.toString()).filter(_.isPK).map(_.attributename).result
    val pkfield = Await.result(DB.run(query), MAX_WAITING_TIME).head

    getAttributes(entityname, Some(pkfield)).head
  }

  /**
    * Returns the attributes of an entity.
    *
    * @param entityname name of entity
    * @param nameFilter filter for attribute name
    * @return
    */
  def getAttributes(entityname: EntityName, nameFilter: Option[String] = None): Seq[AttributeDefinition] = {
    val attributesQuery = if (nameFilter.isDefined) {
      _attributes.filter(_.entityname === entityname.toString()).filter(_.attributename === nameFilter.get).result
    } else {
      _attributes.filter(_.entityname === entityname.toString()).result
    }

    val attributes = Await.result(DB.run(attributesQuery), MAX_WAITING_TIME)


    val attributeOptionsQuery = if (nameFilter.isDefined) {
      _attributeOptions.filter(_.entityname === entityname.toString()).filter(_.attributename === nameFilter.get).result
    } else {
      _attributeOptions.filter(_.entityname === entityname.toString()).result
    }

    val options = Await.result(DB.run(attributeOptionsQuery), MAX_WAITING_TIME).groupBy(_._2).mapValues(_.map(x => x._3 -> x._4).toMap)

    attributes.map(x => {
      val name = x._2
      val fieldtype = FieldTypes.fromString(x._3)
      val pk = x._4
      val unique = x._5
      val indexed = x._6
      val handlerName = x._7
      val params = options.getOrElse(name, Map())

      AttributeDefinition(name, fieldtype, pk, unique, indexed, Some(handlerName), params)
    })
  }


  /**
    * Lists all entities in catalog.
    *
    * @return name of entities
    */
  def listEntities(): Seq[EntityName] = {
    Await.result(DB.run(_entitites.map(_.entityname).result), MAX_WAITING_TIME).map(EntityNameHolder(_))
  }


  /**
    * Checks whether index exists in catalog.
    *
    * @param indexname name of index
    * @return
    */
  def existsIndex(indexname: IndexName): Boolean = {
    val count = Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).length.result), MAX_WAITING_TIME)

    count > 0
  }

  /**
    * Creates index in catalog.
    *
    * @param indexname  name of index
    * @param entityname name of entity
    * @param indexmeta  meta information of index
    */
  def createIndex(indexname: IndexName, entityname: EntityName, attributename: String, indextypename: IndexTypeName, indexmeta: Serializable): Boolean = {
    try {
      if (!existsEntity(entityname)) {
        throw new EntityNotExistingException()
      }

      if (existsIndex(indexname)) {
        throw new IndexExistingException()
      }

      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(indexmeta)
      val meta = bos.toByteArray
      oos.close()
      bos.close()


      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      actions += _indexes.+=((indexname, entityname, attributename, indextypename.name, meta, true))
      actions += _indexweights.+=(indexname, DEFAULT_WEIGHT)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

      log.debug("created index in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
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

    try {
      //on delete cascade...
      Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).delete), MAX_WAITING_TIME)

      log.debug("dropped index " + indexname.toString + " from catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }


  /**
    * Lists all indexes in catalog.
    *
    * @param entityname    filter by entityname, set to null for not using filter
    * @param indextypename filter by indextypename, set to null for not using filter
    * @return
    */
  def listIndexes(entityname: Option[EntityName] = None, indextypename: Option[IndexTypeName] = None): Seq[(IndexName, IndexTypeName, Float)] = {
    val filter = if (entityname.isDefined && indextypename.isDefined) {
      _indexes.filter(_.entityname === entityname.get.toString()).filter(_.indextypename === indextypename.get.name)
    } else if (entityname.isDefined) {
      _indexes.filter(_.entityname === entityname.get.toString())
    } else if (indextypename.isDefined) {
      _indexes.filter(_.indextypename === indextypename.get.name)
    } else {
      _indexes
    }

    val query = filter.join(_indexweights).on(_.indexname === _.indexname).map { case (index, weights) => (index.indexname, index.indextypename, weights.weight) }.result
    Await.result(DB.run(query), MAX_WAITING_TIME).map(index => (EntityNameHolder(index._1), IndexTypes.withName(index._2).get, index._3))
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def getIndexAttribute(indexname: IndexName): String = {
    Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).map(_.attribute).result.head), MAX_WAITING_TIME)
  }

  /**
    * Returns meta information to a specified index.
    *
    * @param indexname name of index
    * @return
    */
  def getIndexMeta(indexname: IndexName): Try[Any] = {
    try {
      val query = _indexes.filter(_.indexname === indexname.toString).map(_.meta).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)

      val bis = new ByteArrayInputStream(data)
      val ois = new ObjectInputStream(bis)
      Success(ois.readObject())
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Returns type name of index
    *
    * @param indexname name of index
    * @return
    */
  def getIndexTypeName(indexname: IndexName): IndexTypeName = {
    val result = Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).map(_.indextypename).result.head), MAX_WAITING_TIME)
    IndexTypes.withName(result).get
  }

  /**
    *
    * @param indexname name of index
    * @param newWeight specify the new weight for the index (the higher the more important), if no weight is
    *                  specified the default weight is used
    * @return
    */
  def updateIndexWeight(indexname: IndexName, newWeight: Option[Float] = Some(DEFAULT_WEIGHT)): Boolean = {
    try {
      Await.result(DB.run(_indexweights.filter(_.indexname === indexname.toString).map(_.weight).update(newWeight.getOrElse(DEFAULT_WEIGHT))), MAX_WAITING_TIME)
      log.debug("updated weight in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def isIndexUptodate(indexname: IndexName): Boolean = {
    Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).map(_.isUpToDate).result.head), MAX_WAITING_TIME)
  }


  /**
    * Mark indexes for given entity stale.
    *
    * @param indexname name of index
    */
  def updateIndexToStale(indexname: IndexName): Boolean = {
    try {
      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      actions += _indexes.filter(_.indexname === indexname.toString).map(_.isUpToDate).update(false)
      actions += _indexweights.filter(_.indexname === indexname.toString).map(_.weight).update(0.toFloat)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      log.debug("made indexes stale in catalog")
      true
    } catch {
      case e: Exception =>
        log.error("error in catalog", e)
        false
    }
  }

  /**
    * Returns the name of the entity corresponding to the index name
    *
    * @param indexname name of index
    * @return
    */
  def getEntitynameFromIndex(indexname: IndexName): EntityName = {
    val name = Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).map(_.entityname).result.head), MAX_WAITING_TIME)
    EntityNameHolder(name)
  }
}
