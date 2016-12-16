package org.vitrivr.adampro.entity

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.vitrivr.adampro.catalog.CatalogOperator
import org.vitrivr.adampro.config.{AdamConfig, FieldNames}
import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.datatypes.FieldTypes._
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.{EntityExistingException, EntityNotExistingException, EntityNotProperlyDefinedException, GeneralAdamException}
import org.vitrivr.adampro.helpers.partition.PartitionMode
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.storage.StorageHandler
import org.vitrivr.adampro.storage.engine.ParquetEngine
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
//TODO: make entities singleton? lock on entity?
case class Entity(val entityname: EntityName)(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  val mostRecentVersion = this.synchronized {
    if (!ac.entityVersion.contains(entityname)) {
      ac.entityVersion.+=(entityname.toString -> ac.sc.accumulator(0L, entityname.toString))
    }

    ac.entityVersion.get(entityname.toString).get
  }
  var currentVersion = mostRecentVersion.value

  /**
    * Gets the primary key.
    *
    * @return
    */
  val pk = CatalogOperator.getPrimaryKey(entityname).get

  private var _schema: Option[Seq[AttributeDefinition]] = None

  /**
    * Schema of the entity.
    *
    * @return
    */
  def schema(nameFilter: Option[Seq[String]] = None, typeFilter: Option[Seq[FieldType]] = None): Seq[AttributeDefinition] = {
    checkVersions()

    if (_schema.isEmpty) {
      _schema = Some(CatalogOperator.getAttributes(entityname).get)
    }

    var tmpSchema = _schema.get

    if (nameFilter.isDefined) {
      tmpSchema = tmpSchema.filter(attribute => nameFilter.get.contains(attribute.name))
    }

    if (typeFilter.isDefined) {
      tmpSchema = tmpSchema.filter(attribute => typeFilter.get.map(_.name).contains(attribute.fieldtype.name))
    }

    tmpSchema
  }

  private var _data: Option[DataFrame] = None

  /**
    * Gets the full entity data.
    *
    * @param nameFilter    filters for names
    * @param typeFilter    filters for field types
    * @param handlerFilter filters for storage handler
    * @param predicates    attributename -> predicate (will only be applied if supported by handler)
    * @return
    */
  def getData(nameFilter: Option[Seq[String]] = None, typeFilter: Option[Seq[FieldType]] = None, handlerFilter: Option[StorageHandler] = None, predicates: Seq[Predicate] = Seq()): Option[DataFrame] = {
    checkVersions()

    //cache data join
    var data = _data

    if (_data.isEmpty && predicates.isEmpty) {
      val handlerData = schema().filterNot(_.pk).groupBy(_.storagehandler).map { case (handler, attributes) =>
        val status = handler.read(entityname, attributes.+:(pk))

        if (status.isFailure) {
          log.error("failure when reading data", status.failed.get)
        }

        status
      }.filter(_.isSuccess).map(_.get)

      if (handlerData.nonEmpty) {
        _data = Some(handlerData.reduce(_.join(_, pk.name)).coalesce(AdamConfig.defaultNumberOfPartitions))
      } else {
        _data = None
      }

      data = _data
    }

    if (predicates.nonEmpty) {
      val handlerData = schema().filterNot(_.pk).groupBy(_.storagehandler).map { case (handler, attributes) =>
        val predicate = predicates.filter(p => (p.attribute == pk.name || attributes.map(_.name).contains(p.attribute)))
        val status = handler.read(entityname, attributes, predicate)

        if (status.isFailure) {
          log.error("failure when reading data", status.failed.get)
        }

        status
      }.filter(_.isSuccess).map(_.get)

      if (handlerData.nonEmpty) {
        data = Some(handlerData.reduce(_.join(_, pk.name)).coalesce(AdamConfig.defaultNumberOfPartitions))
      }
    }

    //possibly filter for current call
    var filteredData = data

    if (handlerFilter.isDefined) {
      //filter by handler, name and type
      filteredData = filteredData.map(_.select(schema(nameFilter, typeFilter).filter(a => a.storagehandler.equals(handlerFilter.get)).map(attribute => col(attribute.name)): _*))
    } else if (nameFilter.isDefined || typeFilter.isDefined) {
      //filter by name and type
      filteredData = filteredData.map(_.select(schema(nameFilter, typeFilter).map(attribute => col(attribute.name)): _*))
    }

    if (data.isDefined) {
      import scala.concurrent.ExecutionContext.Implicits.global
      val future = Future {
        try{
          val cachedDF = data.get.persist(StorageLevel.MEMORY_ONLY)
          data = Some(cachedDF)
        } catch {
          case e : Exception => log.error("could not cache data", e)
        }
      }
    }

    filteredData
  }

  /**
    * Gets feature data.
    *
    * @return
    */
  def getFeatureData: Option[DataFrame] = getData(typeFilter = Some(Seq(FEATURETYPE)))

  /**
    * Gets feature attribute and pk attribute; use this for indexing purposes.
    *
    * @param attribute attribute that is indexed
    * @return
    */
  def getAttributeData(attribute: String)(implicit ac: AdamContext): Option[DataFrame] = getData(Some(Seq(pk.name, attribute)))

  /**
    * Caches the data.
    */
  def cache(): Unit = {
    if (_data.isEmpty) {
      getData()
    }

    if (_data.isDefined) {
      _data = Some(_data.get.cache())
    }
  }


  private val COUNT_KEY = "ntuples"

  /**
    * Returns number of elements in the entity.
    *
    * @return
    */
  def count: Long = {
    checkVersions()

    var count = CatalogOperator.getEntityOption(entityname, Some(COUNT_KEY)).get.get(COUNT_KEY).map(_.toLong)


    if (count.isEmpty) {
      if (getData().isDefined) {
        count = Some(getData().get.count())
        CatalogOperator.updateEntityOption(entityname, COUNT_KEY, count.get.toString)
      }
    }

    count.getOrElse(0)
  }


  /**
    * Gives preview of entity.
    *
    * @param k number of elements to show in preview
    * @return
    */
  def show(k: Int): Option[DataFrame] = getData().map(_.limit(k))


  private val N_INSERTS_VACUUMING = "ninserts"
  private val MAX_INSERTS_VACUUMING = "maxinserts"
  private val MAX_INSERTS_BEFORE_REPARTITIONING = CatalogOperator.getEntityOption(entityname, Some(MAX_INSERTS_VACUUMING)).get.get(MAX_INSERTS_VACUUMING).map(_.toInt).getOrElse(500)
  private val MAX_PARTITIONS = 100

  /**
    * Inserts data into the entity.
    *
    * @param data         data to insert
    * @param ignoreChecks whether to ignore checks
    * @return
    */
  def insert(data: DataFrame, ignoreChecks: Boolean = false): Try[Void] = {
    log.trace("inserting data into entity")

    try {
      val ninserts = CatalogOperator.getEntityOption(entityname, Some(N_INSERTS_VACUUMING)).get.get(N_INSERTS_VACUUMING).map(_.toInt).getOrElse(0)

      val insertion =
        if (pk.fieldtype.equals(FieldTypes.AUTOTYPE)) {
          if (data.schema.fieldNames.contains(pk.name)) {
            return Failure(new GeneralAdamException("the field " + pk.name + " has been specified as auto and should therefore not be provided"))
          }

          val rdd = data.rdd.zipWithUniqueId.map { case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }
          ac.sqlContext
            .createDataFrame(
              rdd, StructType(StructField(pk.name, pk.fieldtype.datatype) +: data.schema.fields))
        } else {
          data
        }.repartition(AdamConfig.defaultNumberOfPartitions)

      //TODO: check insertion schema and entity schema before trying to insert
      //TODO: block on other inserts

      val handlers = insertion.schema.fields
        .map(field => schema(Some(Seq(field.name)))).filterNot(_.isEmpty).map(_.head)
        .filterNot(_.pk)
        .groupBy(_.storagehandler)

      handlers.foreach { case (handler, attributes) =>
        val fields = if (!attributes.exists(_.name == pk.name)) {
          attributes.+:(pk)
        } else {
          attributes
        }

        val df = insertion.select(fields.map(attribute => col(attribute.name)): _*)
        //TODO: check here for PK!
        val status = handler.write(entityname, df, fields, SaveMode.Append, Map("allowRepartitioning" -> "true", "partitioningKey" -> pk.name))

        if (status.isFailure) {
          throw status.failed.get
        }
      }

      if (ninserts < MAX_INSERTS_BEFORE_REPARTITIONING) {
        CatalogOperator.updateEntityOption(entityname, N_INSERTS_VACUUMING, (ninserts + 1).toString)
      } else {
        log.info("number of inserts necessitates now re-partitioning")

        if (schema().filter(_.storagehandler.engine.isInstanceOf[ParquetEngine]).nonEmpty) {
          //entity is partitionable
          EntityPartitioner(this, AdamConfig.defaultNumberOfPartitions, None, None, PartitionMode.REPLACE_EXISTING) //this resets insertion counter
        } else {
          //entity is not partitionable, increase number of partitions to max value
          CatalogOperator.updateEntityOption(entityname, MAX_INSERTS_VACUUMING, Int.MaxValue.toString)
        }
      }

      indexes.map(_.map(_.markStale()))

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    } finally {

      //TODO: possibly clean this
      if (getFeatureData.isDefined) {
        val npartitions = getFeatureData.get.rdd.getNumPartitions

        if (npartitions > MAX_PARTITIONS && schema().filter(_.storagehandler.engine.isInstanceOf[ParquetEngine]).nonEmpty) {
          //entity is partitionable
          EntityPartitioner(this, AdamConfig.defaultNumberOfPartitions, None, None, PartitionMode.REPLACE_EXISTING) //this resets insertion counter
        }
      }

      markStale()
    }
  }

  /**
    *
    */
  private[entity] def resetInsertionCounter(): Unit = {
    CatalogOperator.deleteEntityOption(entityname, N_INSERTS_VACUUMING)
  }


  /**
    *
    * @param predicates
    */
  def delete(predicates: Seq[Predicate]): Int = {
    var newData = getData().get

    predicates.foreach { predicate =>
      newData = newData.filter("NOT " + predicate.sqlString)
    }

    val handlers = newData.schema.fields
      .map(field => schema(Some(Seq(field.name)))).filterNot(_.isEmpty).map(_.head)
      .filterNot(_.pk)
      .groupBy(_.storagehandler)

    handlers.foreach { case (handler, attributes) =>
      val fields = if (!attributes.exists(_.name == pk.name)) {
        attributes.+:(pk)
      } else {
        attributes
      }

      val df = newData.select(fields.map(attribute => col(attribute.name)): _*)
      val status = handler.write(entityname, df, fields, SaveMode.Overwrite, Map("allowRepartitioning" -> "true", "partitioningKey" -> pk.name))

      if (status.isFailure) {
        throw status.failed.get
      }
    }

    val oldCount = count

    markStale

    val newCount = count

    (oldCount - newCount).toInt
  }

  /**
    * Returns all available indexes for the entity.
    *
    * @return
    */
  def indexes: Seq[Try[Index]] = {
    CatalogOperator.listIndexes(Some(entityname)).get.map(index => Index.load(index))
  }

  /**
    * Marks the data stale (e.g., if new data has been inserted to entity).
    */
  def markStale(): Unit = {
    this.synchronized {
      mostRecentVersion += 1

      _schema = None
      _data.map(_.unpersist())
      _data = None

      CatalogOperator.deleteEntityOption(entityname, COUNT_KEY)

      currentVersion = mostRecentVersion.value
    }
  }

  /**
    * Checks if cached data is up to date, i.e. if version of local entity corresponds to global version of entity
    */
  private def checkVersions(): Unit = {
    if (currentVersion < mostRecentVersion.value) {
      log.trace("no longer most up to date version, marking stale")
      markStale()
    }
  }


  /**
    * Drops the data of the entity.
    */
  def drop(): Unit = {
    Index.dropAll(entityname)

    try {
      schema().filterNot(_.pk).groupBy(_.storagehandler)
        .foreach { case (handler, attributes) =>
          try {
            handler.drop(entityname)
          } catch {
            case e: Exception =>
              log.error("exception when dropping entity " + entityname, e)
          }
        }

    } catch {
      case e: Exception =>
        log.error("exception when dropping entity " + entityname, e)
    } finally {
      CatalogOperator.dropEntity(entityname)
    }
  }

  /**
    * Returns stored entity options.
    */
  def options = CatalogOperator.getEntityOption(entityname)

  /**
    * Returns a map of properties to the entity. Useful for printing.
    *
    * @param options
    */
  def propertiesMap(options: Map[String, String] = Map()) = {
    val lb = ListBuffer[(String, String)]()

    lb.append("attributes" -> CatalogOperator.getAttributes(entityname).get.map(field => field.name).mkString(","))
    lb.append("indexes" -> CatalogOperator.listIndexes(Some(entityname)).get.mkString(","))
    lb.append("partitions" -> getFeatureData.map(_.rdd.getNumPartitions.toString).getOrElse("none"))

    if (!(options.contains("count") && options("count") == "false")) {
      lb.append("count" -> count.toString)
    }

    lb.toMap
  }

  /**
    * Returns a map of properties to a specified attribute. Useful for printing.
    *
    * @param attribute name of attribute
    * @param options
    * @return
    */
  def attributePropertiesMap(attribute: String, options: Map[String, String] = Map()): Map[String, String] = {
    schema(Some(Seq(attribute))).headOption.map(_.propertiesMap).getOrElse(Map())
  }


  override def equals(that: Any): Boolean =
    that match {
      case that: Entity =>
        this.entityname.equals(that.entityname)
      case _ => false
    }

  override def hashCode: Int = entityname.hashCode
}

object Entity extends Logging {
  type EntityName = EntityNameHolder

  /**
    * Check if entity exists. Note that this only checks the catalog; the entity may still exist in the file system.
    *
    * @param entityname name of entity
    * @return
    */
  def exists(entityname: EntityName): Boolean = {
    val res = CatalogOperator.existsEntity(entityname)

    if (res.isFailure) {
      throw res.failed.get
    }

    res.get
  }

  /**
    * Creates an entity.
    *
    * @param entityname         name of the entity
    * @param creationAttributes attributes of entity
    * @param ifNotExists        if set to true and the entity exists, the entity is just returned; otherwise an error is thrown
    * @return
    */
  def create(entityname: EntityName, creationAttributes: Seq[AttributeDefinition], ifNotExists: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    try {
      //checks
      if (exists(entityname)) {
        if (!ifNotExists) {
          return Failure(EntityExistingException())
        } else {
          return load(entityname)
        }
      }

      if (creationAttributes.isEmpty) {
        return Failure(EntityNotProperlyDefinedException("Entity " + entityname + " will have no attributes"))
      }

      FieldNames.reservedNames.foreach { reservedName =>
        if (creationAttributes.map(_.name).contains(reservedName)) {
          return Failure(EntityNotProperlyDefinedException("Entity defined with field " + reservedName + ", but name is reserved"))
        }
      }

      if (creationAttributes.map(_.name).distinct.length != creationAttributes.length) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with duplicate fields."))
      }

      val allowedPkTypes = FieldTypes.values.filter(_.pk)

      if (creationAttributes.count(_.pk) > 1) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with more than one primary key"))
      } else if (!creationAttributes.exists(_.pk)) {
        return Failure(EntityNotProperlyDefinedException("Entity defined has no primary key."))
      } else if (!creationAttributes.filter(_.pk).forall(field => allowedPkTypes.contains(field.fieldtype))) {
        return Failure(EntityNotProperlyDefinedException("Entity defined needs a " + allowedPkTypes.map(_.name).mkString(", ") + " primary key"))
      }

      if (creationAttributes.count(_.fieldtype == AUTOTYPE) > 1) {
        return Failure(EntityNotProperlyDefinedException("Too many auto attributes defined."))
      } else if (creationAttributes.count(_.fieldtype == AUTOTYPE) > 0 && !creationAttributes.filter(_.pk).forall(_.fieldtype == AUTOTYPE)) {
        return Failure(EntityNotProperlyDefinedException("Auto type only allowed in primary key."))
      }

      CatalogOperator.createEntity(entityname, creationAttributes)

      val pk = creationAttributes.filter(_.pk)
      val attributesWithoutPK = creationAttributes.filterNot(_.pk)

      if (attributesWithoutPK.isEmpty) {
        //only pk attribute is available
        pk.groupBy(_.storagehandler).foreach {
          case (handler, attributes) =>
            val status = handler.create(entityname, attributes)
            if (status.isFailure) {
              throw new GeneralAdamException("failing on handler " + handler.name + ":" + status.failed.get)
            }
        }
      } else {
        attributesWithoutPK.filterNot(_.pk).groupBy(_.storagehandler).foreach {
          case (handler, attributes) =>
            val status = handler.create(entityname, attributes.++:(pk))
            if (status.isFailure) {
              throw new GeneralAdamException("failing on handler " + handler.name + ":" + status.failed.get)
            }
        }
      }

      Success(Entity(entityname)(ac))
    } catch {
      case e: Exception =>
        //drop everything created in handlers
        ac.storageHandlerRegistry.value.handlers.values.foreach {
          handler =>
            try {
              handler.drop(entityname)
            } catch {
              case e: Exception => //careful: if entity has not been created yet in handler then we may get an exception
            }
        }

        //drop from catalog
        CatalogOperator.dropEntity(entityname, true)

        Failure(e)
    }
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list: Seq[EntityName] = CatalogOperator.listEntities().get


  /**
    * Loads an entity.
    *
    * @param entityname name of entity
    * @return
    */
  def load(entityname: EntityName, cache: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    val entity = ac.entityLRUCache.value.get(entityname)

    if (cache) {
      entity.map(_.cache())
    }

    entity
  }


  /**
    * Loads the entityname metadata without loading the data itself yet.
    *
    * @param entityname name of entity
    * @return
    */
  private[entity] def loadEntityMetaData(entityname: EntityName)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException())
    }

    try {
      Success(Entity(entityname)(ac))
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    * Drops an entity.
    *
    * @param entityname name of entity
    * @param ifExists   if set to true, no error is raised if entity does not exist
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    try {
      if (!exists(entityname)) {
        if (!ifExists) {
          return Failure(EntityNotExistingException())
        } else {
          return Success(null)
        }
      }

      Entity.load(entityname).get.drop()
      ac.entityLRUCache.value.invalidate(entityname)

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}