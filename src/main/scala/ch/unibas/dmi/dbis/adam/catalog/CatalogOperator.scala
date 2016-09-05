package ch.unibas.dmi.dbis.adam.catalog

import java.io._

import ch.unibas.dmi.dbis.adam.catalog.catalogs._
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception._
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.utils.Logging
import slick.dbio.NoStream
import slick.driver.H2Driver.api._ //TODO: this is wrong, but Slick will do weird things if Derby driver is used

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
  private val MAX_WAITING_TIME: Duration = 100.seconds
  private val DB = Database.forURL("jdbc:derby:" + AdamConfig.internalsPath + "/ap_catalog" + ";create=true")

  private[catalog] val SCHEMA = "adampro"

  private val _entitites = TableQuery[EntityCatalog]
  private val _entityOptions = TableQuery[EntityOptionsCatalog]
  private val _attributes = TableQuery[AttributeCatalog]
  private val _attributeOptions = TableQuery[AttributeOptionsCatalog]
  private val _indexes = TableQuery[IndexCatalog]
  private val _indexOptions = TableQuery[IndexOptionsCatalog]
  private val _measurements = TableQuery[MeasurementCatalog]

  private[catalog] val CATALOGS = Seq(
    _entitites, _entityOptions, _attributes, _attributeOptions, _indexes, _indexOptions, _measurements
  )

  /**
    * Initializes the catalog. Method is called at the beginning (see below).
    */
  private def init() {
    try {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      val schemaExists = Await.result(DB.run(sql"""SELECT COUNT(*) FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '#$SCHEMA'""".as[Int]), MAX_WAITING_TIME).headOption

      if(schemaExists.isEmpty || schemaExists.get == 0){
        //schema might not exist yet
        actions += sqlu"""CREATE SCHEMA #$SCHEMA"""
      }

      val tables = Await.result(DB.run(sql"""SELECT TABLENAME FROM SYS.SYSTABLES NATURAL JOIN SYS.SYSSCHEMAS WHERE SCHEMANAME = '#$SCHEMA'""".as[String]), MAX_WAITING_TIME).toSeq

      CATALOGS.foreach { catalog =>
        if (!tables.contains(catalog.baseTableRow.tableName)) {
          actions += catalog.schema.create
        } else {
        }
      }

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
    } catch {
      case e: Exception =>
        log.error("fatal error when creating catalogs", e)
        System.exit(1)
        throw new GeneralAdamException("fatal error when creating catalogs")
    }
  }

  init()

  /**
    * Executes operation.
    *
    * @param desc description to display in log
    * @param op   operation to perform
    * @return
    */
  private def execute[T](desc: String)(op: => T): Try[T] = {
    try {
      log.trace("performed catalog operation: " + desc)
      Success(op)
    } catch {
      case e: Exception =>
        log.error("error in catalog operation: " + desc, e)
        Failure(e)
    }
  }


  /**
    *
    * @param entityname name of entity
    * @param attributes attributes
    * @return
    */
  def createEntity(entityname: EntityName, attributes: Seq[AttributeDefinition]): Try[Void] = {
    execute("create entity") {
      if (existsEntity(entityname).get) {
        throw new EntityExistingException()
      }

      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      actions += _entitites.+=(entityname)

      attributes.foreach { attribute =>
        actions += _attributes.+=(entityname, attribute.name, attribute.fieldtype.name, attribute.pk, attribute.unique, attribute.indexed, attribute.storagehandler.map(_.name).getOrElse(""))

        attribute.params.foreach { case (key, value) =>
          actions += _attributeOptions.+=(entityname, attribute.name, key, value)
        }
      }

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }


  /**
    * Returns the metadata to an entity.
    *
    * @param entityname name of entity
    * @param key        name of key
    * @return
    */
  def getEntityOption(entityname: EntityName, key: Option[String] = None): Try[Map[String, String]] = {
    execute("get entity option") {
      var query = _entityOptions.filter(_.entityname === entityname.toString)

      if (key.isDefined) {
        query = query.filter(_.key === key.get)
      }

      val result = Await.result(DB.run(query.map(a => a.key -> a.value).result), MAX_WAITING_TIME)
      result.toMap
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
  def updateEntityOption(entityname: EntityName, key: String, newValue: String): Try[Void] = {
    execute("update entity option") {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      //upsert
      actions += _entityOptions.filter(_.entityname === entityname.toString()).filter(_.key === key).delete
      actions += _entityOptions.+=(entityname, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Deletes the metadata to an entity.
    *
    * @param entityname name of entity
    * @param key        name of key
    * @return
    */
  def deleteEntityOption(entityname: EntityName, key: String): Try[Void] = {
    execute("delete entity") {
      val query = _entityOptions.filter(_.entityname === entityname.toString).filter(_.key === key).delete
      Await.result(DB.run(query), MAX_WAITING_TIME)
      null
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
  def getAttributeOption(entityname: EntityName, attribute: String, key: Option[String] = None): Try[Map[String, String]] = {
    execute("get attribute option") {
      var query = _attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute)

      if (key.isDefined) {
        query = query.filter(_.key === key.get)
      }

      val result = Await.result(DB.run(query.map(a => a.key -> a.value).result), MAX_WAITING_TIME)
      result.toMap
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
  def updateAttributeOption(entityname: EntityName, attribute: String, key: String, newValue: String): Try[Void] = {
    execute("update attribute option") {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      //upsert
      actions += _attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).filter(_.key === key).delete
      actions += _attributeOptions.+=(entityname, attribute, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
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
  def deleteAttributeOption(entityname: EntityName, attribute: String, key: String): Try[Void] = {
    execute("delete attribute option") {
      val query = _attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).filter(_.key === key).delete
      Await.result(DB.run(query), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Gets the metadata to an index.
    *
    * @param indexname name of index
    * @param key       name of key
    * @return
    */
  def getIndexOption(indexname: IndexName, key: Option[String] = None): Try[Map[String, String]] = {
    execute("get index option") {
      var query = _indexOptions.filter(_.indexname === indexname.toString)

      if (key.isDefined) {
        query = query.filter(_.key === key.get)
      }

      val result = Await.result(DB.run(query.map(a => a.key -> a.value).result), MAX_WAITING_TIME)
      result.toMap
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
  def updateIndexOption(indexname: IndexName, key: String, newValue: String): Try[Void] = {
    execute("update index option") {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      //upsert
      actions += _indexOptions.filter(_.indexname === indexname.toString).filter(_.key === key).delete
      actions += _indexOptions.+=(indexname.toString, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Deletes the metadata to an index.
    *
    * @param indexname name of index
    * @param key       name of key
    * @return
    */
  def deleteIndexOption(indexname: IndexName, key: String): Try[Void] = {
    execute("delete index option") {
      val query = _indexOptions.filter(_.indexname === indexname.toString).filter(_.key === key).delete
      Await.result(DB.run(query), MAX_WAITING_TIME)
      null
    }
  }


  /**
    * Drops entity from catalog.
    *
    * @param entityname name of entity
    * @param ifExists   if true: no error if does not exist
    */
  def dropEntity(entityname: EntityName, ifExists: Boolean = false): Try[Void] = {
    execute("drop entity") {
      if (!existsEntity(entityname).get) {
        if (!ifExists) {
          throw new EntityNotExistingException()
        }
      }

      //on delete cascade...
      Await.result(DB.run(_entitites.filter(_.entityname === entityname.toString()).delete), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Checks whether entity exists in catalog.
    *
    * @param entityname name of entity
    * @return
    */
  def existsEntity(entityname: EntityName): Try[Boolean] = {
    execute("exists entity") {
      val query = _entitites.filter(_.entityname === entityname.toString()).length.result
      Await.result(DB.run(query), MAX_WAITING_TIME) > 0
    }
  }

  /**
    *
    * @param entityname name of entity
    * @return
    */
  def getPrimaryKey(entityname: EntityName): Try[AttributeDefinition] = {
    execute("get primary key") {
      val query = _attributes.filter(_.entityname === entityname.toString()).filter(_.isPK).map(_.attributename).result
      val pkfield = Await.result(DB.run(query), MAX_WAITING_TIME).head

      getAttributes(entityname, Some(pkfield)).get.head
    }
  }

  /**
    * Returns the attributes of an entity.
    *
    * @param entityname name of entity
    * @param nameFilter filter for attribute name
    * @return
    */
  def getAttributes(entityname: EntityName, nameFilter: Option[String] = None): Try[Seq[AttributeDefinition]] = {
    execute("get attributes") {
      val attributesQuery = if (nameFilter.isDefined) {
        _attributes.filter(_.entityname === entityname.toString()).filter(_.attributename === nameFilter.get).result
      } else {
        _attributes.filter(_.entityname === entityname.toString()).result
      }

      val attributes = Await.result(DB.run(attributesQuery), MAX_WAITING_TIME)

      val attributeDefinitions = attributes.map(x => {
        val name = x._2
        val fieldtype = FieldTypes.fromString(x._3)
        val pk = x._4
        val unique = x._5
        val indexed = x._6
        val handlerName = x._7

        val attributeOptionsQuery = _attributeOptions.filter(_.entityname === entityname.toString()).filter(_.attributename === name).result
        val options = Await.result(DB.run(attributeOptionsQuery), MAX_WAITING_TIME).groupBy(_._2).mapValues(_.map(x => x._3 -> x._4).toMap)

        val params = options.getOrElse(name, Map())

        AttributeDefinition(name, fieldtype, pk, unique, indexed, Some(handlerName), params)
      })

      attributeDefinitions
    }
  }


  /**
    * Lists all entities in catalog.
    *
    * @return name of entities
    */
  def listEntities(): Try[Seq[EntityName]] = {
    execute("list entities") {
      val query = _entitites.map(_.entityname).result
      Await.result(DB.run(query), MAX_WAITING_TIME).map(EntityNameHolder(_))
    }
  }


  /**
    * Checks whether index exists in catalog.
    *
    * @param indexname name of index
    * @return
    */
  def existsIndex(indexname: IndexName): Try[Boolean] = {
    execute("exists index") {
      val query = _indexes.filter(_.indexname === indexname.toString).length.result
      Await.result(DB.run(query), MAX_WAITING_TIME) > 0
    }
  }

  /**
    * Checks whether index exists in catalog.
    *
    * @param entityname name of index
    * @param attribute  name of attribute
    * @param indextypename
    * @return
    */
  def existsIndex(entityname: EntityName, attribute: String, indextypename: IndexTypeName): Try[Boolean] = {
    execute("exists index") {
      val query = _indexes.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).filter(_.indextypename === indextypename.toString).length.result
      Await.result(DB.run(query), MAX_WAITING_TIME) > 0
    }
  }

  /**
    * Creates index in catalog.
    *
    * @param indexname  name of index
    * @param entityname name of entity
    * @param indexmeta  meta information of index
    */
  def createIndex(indexname: IndexName, entityname: EntityName, attributename: String, indextypename: IndexTypeName, indexmeta: Serializable): Try[Void] = {
    execute("create index") {
      if (!existsEntity(entityname).get) {
        throw new EntityNotExistingException()
      }

      if (existsIndex(indexname).get) {
        throw new IndexExistingException()
      }

      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(indexmeta)
      val meta = bos.toByteArray
      oos.close()
      bos.close()

      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      actions += _indexes.+=((indexname, entityname, attributename, indextypename.name, meta, true))

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Drops index from catalog.
    *
    * @param indexname name of index
    * @param ifExists  if true: no error if does not exist
    * @return
    */
  def dropIndex(indexname: IndexName, ifExists: Boolean = false): Try[Void] = {
    execute("drop index") {
      if (!existsIndex(indexname).get) {
        if (!ifExists) {
          throw new IndexNotExistingException()
        }
      }

      //on delete cascade...
      Await.result(DB.run(_indexes.filter(_.indexname === indexname.toString).delete), MAX_WAITING_TIME)

      null
    }
  }


  /**
    * Lists all indexes in catalog.
    *
    * @param entityname    filter by entityname, set to null for not using filter
    * @param indextypename filter by indextypename, set to null for not using filter
    * @return
    */
  def listIndexes(entityname: Option[EntityName] = None, indextypename: Option[IndexTypeName] = None): Try[Seq[IndexName]] = {
    execute("list indexes") {
      val filter = if (entityname.isDefined && indextypename.isDefined) {
        _indexes.filter(_.entityname === entityname.get.toString()).filter(_.indextypename === indextypename.get.name)
      } else if (entityname.isDefined) {
        _indexes.filter(_.entityname === entityname.get.toString())
      } else if (indextypename.isDefined) {
        _indexes.filter(_.indextypename === indextypename.get.name)
      } else {
        _indexes
      }

      val query = filter.map(_.indexname).result
      Await.result(DB.run(query), MAX_WAITING_TIME).map(EntityNameHolder(_))
    }
  }

  /**
    *
    * @param indexname name of index
    * @return
    */
  def getIndexAttribute(indexname: IndexName): Try[String] = {
    execute("get index attribute") {
      val query = _indexes.filter(_.indexname === indexname.toString).map(_.attributename).result.head
      Await.result(DB.run(query), MAX_WAITING_TIME)
    }
  }

  /**
    * Returns meta information to a specified index.
    *
    * @param indexname name of index
    * @return
    */
  def getIndexMeta(indexname: IndexName): Try[Any] = {
    execute("get index meta") {
      val query = _indexes.filter(_.indexname === indexname.toString).map(_.meta).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)

      val bis = new ByteArrayInputStream(data)
      val ois = new ObjectInputStream(bis)
      ois.readObject()
    }
  }

  /**
    * Returns type name of index
    *
    * @param indexname name of index
    * @return
    */
  def getIndexTypeName(indexname: IndexName): Try[IndexTypeName] = {
    execute("get index type name") {
      val query = _indexes.filter(_.indexname === indexname.toString).map(_.indextypename).result.head
      IndexTypes.withName(Await.result(DB.run(query), MAX_WAITING_TIME)).get
    }
  }


  /**
    *
    * @param indexname name of index
    * @return
    */
  def isIndexUptodate(indexname: IndexName): Try[Boolean] = {
    execute("is index up to date") {
      val query = _indexes.filter(_.indexname === indexname.toString).map(_.isUpToDate).result.head
      Await.result(DB.run(query), MAX_WAITING_TIME)
    }
  }


  /**
    * Mark indexes for given entity stale.
    *
    * @param indexname name of index
    */
  def makeIndexStale(indexname: IndexName): Try[Void] = {
    execute("make index stale") {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      actions += _indexes.filter(_.indexname === indexname.toString).map(_.isUpToDate).update(false)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Returns the name of the entity corresponding to the index name
    *
    * @param indexname name of index
    * @return
    */
  def getEntityName(indexname: IndexName): Try[EntityName] = {
    execute("get entity name") {
      val query = _indexes.filter(_.indexname === indexname.toString).map(_.entityname).result.head
      val name = Await.result(DB.run(query), MAX_WAITING_TIME)
      EntityNameHolder(name)
    }
  }

  /**
    * Adds a measurement to the catalog
    *
    * @param key
    * @param value
    * @return
    */
  def addMeasurement(key: String, value: Long): Try[Void] = {
    execute("add measurement") {
      val query = _measurements.+=(key, value)
      DB.run(query)
      null
    }
  }

  /**
    * Gets measurements for given key.
    *
    * @param key
    * @return
    */
  def getMeasurements(key: String): Try[Seq[Long]] = {
    execute("get measurement") {
      val query = _measurements.filter(_.key === key).map(_.measurement).result
      Await.result(DB.run(query), MAX_WAITING_TIME)
    }
  }

  /**
    * Drops measurements for given key.
    *
    * @param key
    * @return
    */
  def dropMeasurements(key: String): Try[Void] = {
    execute("drop measurements") {
      Await.result(DB.run(_measurements.filter(_.key === key).delete), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Drops measurements for given key.
    *
    * @return
    */
  def dropAllMeasurements(): Try[Void] = {
    execute("drop all measurements") {
      Await.result(DB.run(_measurements.delete), MAX_WAITING_TIME)
      null
    }
  }
}
