package org.vitrivr.adampro.catalog

import java.io._

import com.mchange.v2.c3p0.ComboPooledDataSource
import org.vitrivr.adampro.catalog.catalogs._
import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.entity.{AttributeDefinition, EntityNameHolder}
import org.vitrivr.adampro.exception._
import org.vitrivr.adampro.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.index.partition.CustomPartitioner
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.utils.Logging
import slick.dbio.NoStream
import slick.driver.DerbyDriver.api._

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
class CatalogOperator(internalsPath: String) extends Logging {
  private val MAX_WAITING_TIME: Duration = 100.seconds

  try {
    Class.forName("org.apache.derby.jdbc.EmbeddedDriver").newInstance
  } catch {
    case e: Exception => {
      log.error("driver not available for catalog", e.getMessage)
      throw e
    }
  }

  private val ds = new ComboPooledDataSource
  ds.setDriverClass("org.apache.derby.jdbc.EmbeddedDriver")
  ds.setJdbcUrl("jdbc:derby:" + internalsPath + "/ap_catalog" + "")

  private val DB = Database.forDataSource(ds)

  private val _entitites = TableQuery[EntityCatalog]
  private val _entityOptions = TableQuery[EntityOptionsCatalog]
  private val _attributes = TableQuery[AttributeCatalog]
  private val _attributeOptions = TableQuery[AttributeOptionsCatalog]
  private val _indexes = TableQuery[IndexCatalog]
  private val _indexOptions = TableQuery[IndexOptionsCatalog]
  private val _storeengineOptions = TableQuery[StorageEngineOptionsCatalog]
  private val _partitioners = TableQuery[PartitionerCatalog]
  private val _optimizerOptions = TableQuery[OptimizerOptionsCatalog]
  private val _options = TableQuery[OptionsCatalog]

  private[catalog] val CATALOGS = Seq(
    _entitites, _entityOptions, _attributes, _attributeOptions, _indexes, _indexOptions, _storeengineOptions, _partitioners, _optimizerOptions, _options
  )

  /**
    * Initializes the catalog. Method is called at the beginning (see below).
    */
  private def init() {
    val connection = Database.forURL("jdbc:derby:" + internalsPath + "/ap_catalog" + ";create=true")

    try {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      val schema = CatalogOperator.SCHEMA

      val schemaExists = Await.result(connection.run(sql"""SELECT COUNT(*) FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = '#$schema'""".as[Int]), MAX_WAITING_TIME).headOption

      if (schemaExists.isEmpty || schemaExists.get == 0) {
        //schema might not exist yet
        actions += sqlu"""CREATE SCHEMA #$schema"""
      }

      val tables = Await.result(connection.run(sql"""SELECT TABLENAME FROM SYS.SYSTABLES NATURAL JOIN SYS.SYSSCHEMAS WHERE SCHEMANAME = '#$schema'""".as[String]), MAX_WAITING_TIME).toSeq

      CATALOGS.foreach { catalog =>
        if (!tables.contains(catalog.baseTableRow.tableName)) {
          actions += catalog.schema.create
        } else {
        }
      }

      Await.result(connection.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
    } catch {
      case e: Exception =>
        log.error("fatal error when creating catalogs", e)
        System.exit(1)
        throw new GeneralAdamException("fatal error when creating catalogs")
    } finally {
      connection.close()
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
      val res = op
      Success(res)
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
        actions += _attributes.+=(entityname, attribute.name, attribute.attributeType.name, attribute.pk, attribute.storagehandlername)

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
    *
    *
    * @param entityname
    * @param attributename
    * @param newHandlerName
    */
  def updateAttributeStorageHandler(entityname: EntityName, attributename: String, newHandlerName: String): Try[Void] = {
    execute("update storage handler") {
      val query = _attributes.filter(_.entityname === entityname.toString).filter(_.attributename === attributename).result
      val attribute = Await.result(DB.run(query), MAX_WAITING_TIME).head

      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      //update
      actions += _attributes.filter(_.entityname === entityname.toString).filter(_.attributename === attributename).map(_.handlername).update(newHandlerName)
      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
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
    * Gets the metadata to an attribute of an entity.
    *
    * @param entityname name of entity
    * @param attribute  name of attribute
    * @param key        name of key
    * @param z          zero/start value
    * @param op         update method
    * @return
    */
  def getAndUpdateAttributeOption(entityname: EntityName, attribute: String, key: String, z: String, op: (String) => (String)): Try[Map[String, String]] = {
    execute("get attribute option") {
      _attributeOptions.synchronized({
        val query = _attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).filter(_.key === key).map(_.value).result
        val results = Await.result(DB.run(query), MAX_WAITING_TIME)

        val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

        if (results.isEmpty) {
          actions += _attributeOptions.+=(entityname, attribute, key, z)
        } else {
          results.foreach { result =>
            //upsert
            actions += _attributeOptions.filter(_.entityname === entityname.toString).filter(_.attributename === attribute).filter(_.key === key).delete
            actions += _attributeOptions.+=(entityname, attribute, key, op(result))
          }
        }

        Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

        Success(Await.result(DB.run(query), MAX_WAITING_TIME))
      })
    }

    getAttributeOption(entityname, attribute, Some(key))
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
          throw EntityNotExistingException.withEntityname(entityname)
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

      val res = getAttributes(entityname, Some(pkfield)).get.head
      assert(res != null)
      res
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
        val attributetype = AttributeTypes.fromString(x._3)
        val pk = x._4
        val handlerName = x._5

        val attributeOptionsQuery = _attributeOptions.filter(_.entityname === entityname.toString()).filter(_.attributename === name).result
        val options = Await.result(DB.run(attributeOptionsQuery), MAX_WAITING_TIME).groupBy(_._2).mapValues(_.map(x => x._3 -> x._4).toMap)

        val params = options.getOrElse(name, Map())

        AttributeDefinition(name, attributetype, handlerName, params)
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
      val entitiesQuery = _entitites.map(_.entityname).result
      val entities = Await.result(DB.run(entitiesQuery), MAX_WAITING_TIME)

      val indexesQuery = _indexes.map(_.indexname).result
      val indexes = Await.result(DB.run(indexesQuery), MAX_WAITING_TIME)

      (entities diff indexes).map(EntityNameHolder(_))
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
        throw EntityNotExistingException.withEntityname(entityname)
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

      actions += _entitites.+=(indexname) //TODO: clean this

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
          throw IndexNotExistingException.withIndexname(indexname)
        }
      }

      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      actions += _indexes.filter(_.indexname === indexname.toString).delete

      actions += _entitites.filter(_.entityname === indexname.toString).delete //TODO: clean this

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)

      null
    }
  }


  /**
    * Lists all indexes in catalog.
    *
    * @param entityname    filter by entityname, set to null for not using filter
    * @param attribute     filter by attribute
    * @param indextypename filter by indextypename, set to null for not using filter
    * @return
    */
  def listIndexes(entityname: Option[EntityName] = None, attribute: Option[String] = None, indextypename: Option[IndexTypeName] = None): Try[Seq[IndexName]] = {
    execute("list indexes") {
      var filter: Query[IndexCatalog, (String, String, String, String, Array[Byte], Boolean), Seq] = _indexes

      if (entityname.isDefined) {
        filter = filter.filter(_.entityname === entityname.get.toString())
      }

      if (attribute.isDefined) {
        filter = filter.filter(_.attributename === attribute.get)
      }

      if (indextypename.isDefined) {
        filter = filter.filter(_.indextypename === indextypename.get.name)
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
      val query = _indexes.filter(_.indexname === indexname.toString).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)

      val bis = new ByteArrayInputStream(data._5)
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
    * Returns the metadata to a storage engine store.
    *
    * @param engine    name of storage engine
    * @param storename name of storename
    * @param key       name of key
    * @return
    */
  def getStorageEngineOption(engine: String, storename: String, key: Option[String] = None): Try[Map[String, String]] = {
    execute("get storage engine option") {
      var query = _storeengineOptions.filter(_.engine === engine).filter(_.storename === storename)

      if (key.isDefined) {
        query = query.filter(_.key === key.get)
      }

      val result = Await.result(DB.run(query.map(a => a.key -> a.value).result), MAX_WAITING_TIME)
      result.toMap
    }
  }


  /**
    * Updates or inserts a metadata information to a storage engine store.
    *
    * @param engine    name of storage engine
    * @param storename name of storename
    * @param key       name of key
    * @param newValue  new value
    * @return
    */
  def updateStorageEngineOption(engine: String, storename: String, key: String, newValue: String): Try[Void] = {
    execute("update storage engine option") {
      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      //upsert
      actions += _storeengineOptions.filter(_.engine === engine).filter(_.storename === storename).filter(_.key === key).delete
      actions += _storeengineOptions.+=(engine, storename, key, newValue)

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Deletes the metadata to a storage engine store.
    *
    * @param engine    name of storage engine
    * @param storename name of storename
    * @param key       name of key
    * @return
    */
  def deleteStorageEngineOption(engine: String, storename: String, key: Option[String]): Try[Void] = {
    execute("delete storage engine option") {
      var query = _storeengineOptions.filter(_.engine === engine).filter(_.storename === storename)


      if (key.isDefined) {
        query = query.filter(_.key === key.get)
      }

      Await.result(DB.run(query.delete), MAX_WAITING_TIME)
      null
    }
  }


  /**
    * Creates a new partitioner.
    *
    * @param indexname
    * @param noPartitions
    * @param partitionerMeta
    * @param partitioner
    * @return
    */
  def createPartitioner(indexname: EntityNameHolder, noPartitions: Int, partitionerMeta: Serializable, partitioner: CustomPartitioner): Try[Void] = {
    execute("create partitioner") {
      if (!existsIndex(indexname).get) {
        throw IndexNotExistingException.withIndexname(indexname)
      }

      val mbos = new ByteArrayOutputStream()
      val moos = new ObjectOutputStream(mbos)
      moos.writeObject(partitionerMeta)
      val meta = mbos.toByteArray
      moos.close()
      mbos.close()

      val pbos = new ByteArrayOutputStream()
      val poos = new ObjectOutputStream(pbos)
      poos.writeObject(partitioner)
      val part = pbos.toByteArray
      poos.close()
      poos.close()

      val actions: ListBuffer[DBIOAction[_, NoStream, _]] = new ListBuffer()

      actions += _partitioners.+=((indexname, noPartitions, meta, part))

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Returns partitioner metadata for the given index
    *
    * @param indexname
    * @return
    */
  def getPartitionerMeta(indexname: EntityNameHolder): Try[Any] = {
    execute("get partitioner meta") {
      val query = _partitioners.filter(_.indexname === indexname.toString).map(_.meta).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)

      val bis = new ByteArrayInputStream(data)
      val ois = new ObjectInputStream(bis)
      ois.readObject()
    }
  }

  /**
    *
    * @param indexname
    * @return
    */
  def getNumberOfPartitions(indexname: EntityNameHolder): Try[Int] = {
    execute("get number of partitions") {
      val query = _partitioners.filter(_.indexname === indexname.toString).map(_.noPartitions).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)
      data
    }
  }

  /**
    *
    * @param indexname
    * @return
    */
  def getPartitioner(indexname: EntityNameHolder): Try[CustomPartitioner] = {
    execute("get partitioner") {
      val query = _partitioners.filter(_.indexname === indexname.toString).map(_.partitioner).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)
      val bis = new ByteArrayInputStream(data)
      val ois = new ObjectInputStream(bis)
      val obj = ois.readObject()
      obj.asInstanceOf[CustomPartitioner]
    }
  }

  /**
    * Drop partitioner from the catalog.
    *
    * @param indexname
    * @return
    */
  def dropPartitioner(indexname: EntityNameHolder): Try[Void] = {
    execute("drop partitioner") {
      if (!existsIndex(indexname).get) {
        throw IndexNotExistingException.withIndexname(indexname)
      }
      Await.result(DB.run(_partitioners.filter(_.indexname === indexname.toString).delete), MAX_WAITING_TIME)
      null
    }
  }


  /**
    * Create options for optimizer
    *
    * @param optimizer
    * @param key
    * @param optimizermeta
    */
  def createOptimizerOption(optimizer: String, key: String, optimizermeta: Serializable): Try[Void] = {
    execute("create optimizer option") {
      val bos = new ByteArrayOutputStream()
      val oos = new ObjectOutputStream(bos)
      oos.writeObject(optimizermeta)
      val meta = bos.toByteArray
      oos.close()
      bos.close()

      val actions = new ListBuffer[DBIOAction[_, NoStream, _]]()

      actions += _optimizerOptions.+=((optimizer, key, meta))

      Await.result(DB.run(DBIO.seq(actions.toArray: _*).transactionally), MAX_WAITING_TIME)
      null
    }
  }

  /**
    * Update options for optimizer
    *
    * @param optimizer
    * @param key
    * @param optimizermeta
    */
  def updateOptimizerOption(optimizer: String, key: String, optimizermeta: Serializable): Try[Void] = {
    execute("update optimizer option") {
      dropOptimizerOptionMeta(optimizer, key)
      createOptimizerOption(optimizer, key, optimizermeta)
      null
    }
  }

  /**
    *
    * @param optimizer
    * @param key
    * @return
    */
  def containsOptimizerOptionMeta(optimizer: String, key: String): Try[Boolean] = {
    execute("get optimizer meta") {
      val query = _optimizerOptions.filter(_.optimizer === optimizer).filter(_.key === key).map(x => (x.optimizer, x.key)).countDistinct.result
      val count = Await.result(DB.run(query), MAX_WAITING_TIME)

      count > 0
    }
  }

  /**
    *
    * @param optimizer
    * @param key
    * @return
    */
  def getOptimizerOptionMeta(optimizer: String, key: String): Try[Any] = {
    execute("get optimizer meta") {
      val query = _optimizerOptions.filter(_.optimizer === optimizer).filter(_.key === key).result.head
      val data = Await.result(DB.run(query), MAX_WAITING_TIME)

      val bis = new ByteArrayInputStream(data._3)
      val ois = new ObjectInputStream(bis)
      ois.readObject()
    }
  }

  /**
    *
    *
    * @param optimizer
    * @param key
    * @return
    */
  def dropOptimizerOptionMeta(optimizer: String, key: String): Try[Void] = {
    execute("drop optimizer meta") {
      Await.result(DB.run(_optimizerOptions.filter(_.optimizer === optimizer).filter(_.key === key).delete), MAX_WAITING_TIME)
      null
    }
  }
}

object CatalogOperator {
  private[catalog] val SCHEMA = "adampro"
}