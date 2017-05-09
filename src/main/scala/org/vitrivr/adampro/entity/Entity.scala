package org.vitrivr.adampro.entity

import java.util.concurrent.locks.StampedLock

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.AttributeTypes._
import org.vitrivr.adampro.datatypes.{AttributeTypes, TupleID}
import org.vitrivr.adampro.entity.Entity.{AttributeName, EntityName}
import org.vitrivr.adampro.exception.{EntityExistingException, EntityNotExistingException, EntityNotProperlyDefinedException, GeneralAdamException}
import org.vitrivr.adampro.index.Index
import org.vitrivr.adampro.main.{AdamContext, SparkStartup}
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
case class Entity(entityname: EntityName)(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  private val mostRecentVersion = this.synchronized {
    if (!ac.entityVersion.contains(entityname)) {
      ac.entityVersion.+=(entityname.toString -> ac.sc.longAccumulator(entityname.toString + "-version"))
    }

    ac.entityVersion(entityname)
  }
  private var currentVersion = mostRecentVersion.value

  /**
    * Gets the primary key.
    *
    * @return
    */
  val pk: AttributeDefinition = SparkStartup.catalogOperator.getPrimaryKey(entityname).get

  private var _schema: Option[Seq[AttributeDefinition]] = None

  /**
    * Schema of the entity.
    *
    * @param nameFilter filter for name
    * @param typeFilter filter for type
    * @param fullSchema add internal fields as well (e.g. TID)
    * @return
    */
  def schema(nameFilter: Option[Seq[AttributeName]] = None, typeFilter: Option[Seq[AttributeType]] = None, fullSchema: Boolean = true): Seq[AttributeDefinition] = {
    checkVersions()

    if (_schema.isEmpty) {
      _schema = Some(SparkStartup.catalogOperator.getAttributes(entityname).get.filterNot(_.pk))
    }

    var tmpSchema = _schema.get

    if (nameFilter.isDefined) {
      tmpSchema = tmpSchema.filter(attribute => nameFilter.get.contains(attribute.name))
    }

    if (typeFilter.isDefined) {
      tmpSchema = tmpSchema.filter(attribute => typeFilter.get.map(_.name).contains(attribute.attributeType.name))
    }

    if (fullSchema) {
      tmpSchema = tmpSchema.+:(pk)
    }

    tmpSchema
  }

  /**
    *
    */
  private[entity] lazy val handlers = schema(fullSchema = false).map(_.storagehandler()).distinct

  private var _data: Option[DataFrame] = None

  /**
    * Reads entity data and caches.
    *
    * @param persist persist dataframe to memory
    */
  private def readData(persist: Boolean = true): Unit = {
    val handlerData = schema(fullSchema = false).groupBy(_.storagehandler).map { case (handler, attributes) =>
      val status = handler.read(entityname, attributes.+:(pk))

      if (status.isFailure) {
        log.error("failure when reading data", status.failed.get)
      }

      status
    }.filter(_.isSuccess).map(_.get)

    if (handlerData.nonEmpty) {
      if (handlerData.size == 1) {
        _data = Some(handlerData.head)
      } else {
        _data = Some(handlerData.reduce(_.join(_, pk.name)).coalesce(ac.config.defaultNumberOfPartitions))
      }
    } else {
      _data = None
    }

    if (_data.isDefined && persist) {
      import scala.concurrent.ExecutionContext.Implicits.global
      val future = Future[DataFrame] {
        _data.get.persist(StorageLevel.MEMORY_ONLY_SER)
      }
      future.onComplete {
        case Success(cachedDF) => _data = Some(cachedDF)
        case Failure(e) => log.error("could not cache data")
      }
    }
  }


  /**
    * Gets the full entity data.
    *
    * @param nameFilter    filters for names
    * @param typeFilter    filters for field types
    * @param handlerFilter filters for storage handler
    * @param predicates    attributename -> predicate (will only be applied if supported by handler)
    * @return
    */
  def getData(nameFilter: Option[Seq[AttributeName]] = None, typeFilter: Option[Seq[AttributeType]] = None, handlerFilter: Option[StorageHandler] = None, predicates: Seq[Predicate] = Seq()): Option[DataFrame] = {
    checkVersions()

    if (_data.isEmpty) {
      readData()
    }

    var data = _data

    //possibly filter for current call
    var filteredData = data

    //predicates
    if (predicates.nonEmpty && filteredData.isEmpty) {
      val handlerData = schema().filterNot(_.pk).groupBy(_.storagehandler).map { case (handler, attributes) =>
        val predicate = predicates.filter(p => (p.attribute == pk.name || attributes.map(_.name).contains(p.attribute)))
        val status = handler.read(entityname, attributes, predicate.toList)

        if (status.isFailure) {
          log.error("failure when reading data", status.failed.get)
        }

        status
      }.filter(_.isSuccess).map(_.get)

      if (handlerData.nonEmpty) {
        if (handlerData.size == 1) {
          filteredData = Some(handlerData.head)
        } else {
          filteredData = Some(handlerData.reduce(_.join(_, pk.name)).coalesce(ac.config.defaultNumberOfPartitions))
        }
      }
    } else {
      predicates.foreach { predicate =>
        filteredData = filteredData.map(data => data.filter(col(predicate.attribute).isin(predicate.values: _*)))
      }
    }

    //handler
    if (handlerFilter.isDefined) {
      //filter by handler, name and type
      filteredData = filteredData.map(_.select(schema(nameFilter, typeFilter).filter(a => a.storagehandler.equals(handlerFilter.get)).map(attribute => col(attribute.name)): _*))
    }

    //name and type filter
    if (nameFilter.isDefined || typeFilter.isDefined) {
      //filter by name and type
      filteredData = filteredData.map(_.select(schema(nameFilter, typeFilter).map(attribute => col(attribute.name)): _*))
    }

    filteredData
  }

  /**
    * Gets feature data.
    *
    * @return
    */
  def getFeatureData: Option[DataFrame] = getData(typeFilter = Some(Seq(VECTORTYPE, SPARSEVECTORTYPE)))


  /**
    * Returns feature data quickly. Should only be used for internal purposes.
    *
    * @return
    */
  private def getFeatureDataFast: Option[DataFrame] = {
    val handlerData = schema(fullSchema = false).filter(_.storagehandler.engine.isInstanceOf[ParquetEngine]).groupBy(_.storagehandler).map { case (handler, attributes) =>
      val status = handler.read(entityname, attributes.+:(pk))

      if (status.isFailure) {
        log.error("failure when reading data", status.failed.get)
      }

      status
    }.filter(_.isSuccess).map(_.get)

    if (handlerData.nonEmpty) {
      if (handlerData.size == 1) {
        Some(handlerData.head)
      } else {
        Some(handlerData.reduce(_.join(_, pk.name)).coalesce(ac.config.defaultNumberOfPartitions))
      }
    } else {
      None
    }
  }


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
      readData()
    }

    if (_data.isDefined) {
      _data = Some(_data.get.persist(StorageLevel.MEMORY_ONLY))
      _data.get.count() //counting for caching
    }
  }

  /**
    * Returns number of elements in the entity.
    *
    * @return
    */
  def count: Long = {
    checkVersions()

    var count = SparkStartup.catalogOperator.getEntityOption(entityname, Some(Entity.COUNT_KEY)).get.get(Entity.COUNT_KEY).map(_.toLong)

    if (count.isEmpty) {
      if (getData().isDefined) {
        count = Some(getData().get.count())
        SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.COUNT_KEY, count.get.toString)
        SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.APPROX_COUNT_KEY, count.get.toString)
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
  def show(k: Int): Option[DataFrame] = getData().map(_.select(schema(fullSchema = false).map(_.name).map(x => col(x.toString)): _*).limit(k))

  private val MAX_INSERTS_BEFORE_VACUUM = SparkStartup.catalogOperator.getEntityOption(entityname, Some(Entity.MAX_INSERTS_VACUUMING)).get.get(Entity.MAX_INSERTS_VACUUMING).map(_.toInt).getOrElse(Entity.DEFAULT_MAX_INSERTS_BEFORE_VACUUM)

  /**
    * Returns the total number of inserts in entity
    */
  private def totalNumberOfInserts() = {
    SparkStartup.catalogOperator.getEntityOption(entityname, Some(Entity.N_INSERTS)).get.get(Entity.N_INSERTS).map(_.toInt).getOrElse(0)
  }

  /**
    * Returns the total number of inserts in entity since last vacuuming
    */
  private def totalNumberOfInsertsSinceVacuuming() = {
    SparkStartup.catalogOperator.getEntityOption(entityname, Some(Entity.N_INSERTS_VACUUMING)).get.get(Entity.N_INSERTS_VACUUMING).map(_.toInt).getOrElse(0)
  }

  /**
    *
    * @param i increment by
    */
  private def incrementNumberOfInserts(i: Int = 1) = {
    SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.N_INSERTS, (totalNumberOfInserts + i).toString)
    SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.N_INSERTS_VACUUMING, (totalNumberOfInsertsSinceVacuuming + i).toString)
  }

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
      // 12 bits for the last 12 bits of the current nano time
      // 28 bits for the current insertion id, i.e., the number of inserts so far into entity
      // 24 bits for the tuple id within the insertion, i.e., the index within the insertion
      val ninserts = totalNumberOfInserts()
      val ninsertsvacuum = totalNumberOfInsertsSinceVacuuming()
      val currentTimeBits = (System.nanoTime() & 4095) << 52

      val tupleidUDF = udf((count: Long) => {
        val ninsertsBits = ((ninserts.toLong + 1) & 268435455) << 24
        val countBits = (count & 16777215)
        currentTimeBits + ninsertsBits + countBits
      })

      //attach TID to rows
      val rdd = data.rdd.zipWithIndex.map { case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }

      var insertion = ac.sqlContext.createDataFrame(
        rdd, StructType(StructField(AttributeNames.internalIdColumnName + "-tmp", LongType) +: data.schema.fields))

      val insertionSize = insertion.count()

      insertion = insertion.withColumn(AttributeNames.internalIdColumnName, tupleidUDF(col(AttributeNames.internalIdColumnName + "-tmp"))).drop(AttributeNames.internalIdColumnName + "-tmp")

      //AUTOTYPE attributes
      val autoAttributes = schema(typeFilter = Some(Seq(AttributeTypes.AUTOTYPE)), fullSchema = false)
      if (autoAttributes.nonEmpty) {
        if (autoAttributes.map(_.name).exists(x => data.schema.fieldNames.map(AttributeNameHolder(_)).contains(x))) {
          return Failure(new GeneralAdamException("the attributes " + autoAttributes.map(_.name).mkString(", ") + " have been specified as auto and should therefore not be provided"))
        }

        autoAttributes.foreach { attribute =>
          insertion = insertion.withColumn(attribute.name, col(AttributeNames.internalIdColumnName))
        }
      }

      insertion = insertion.repartition(ac.config.defaultNumberOfPartitions)

      //TODO: check insertion schema and entity schema before trying to insert

      val lock = ac.getEntityLock(entityname)
      val stamp = lock.writeLock()

      try {
        //insertion per handler
        val handlers = insertion.schema.fields
          .map(field => schema(Some(Seq(field.name)), fullSchema = false)).filterNot(_.isEmpty).map(_.head)
          .groupBy(_.storagehandler)

        handlers.foreach { case (handler, attributes) =>
          val fields = if (!attributes.exists(_.name == pk.name)) {
            attributes.+:(pk)
          } else {
            attributes
          }

          val df = insertion.select(fields.map(attribute => col(attribute.name)): _*)
          val status = handler.write(entityname, df, fields, SaveMode.Append, Map("allowRepartitioning" -> "true", "partitioningKey" -> pk.name))

          if (status.isFailure) {
            throw status.failed.get
          }
        }

        incrementNumberOfInserts()
        markStale()

        val apxCountOld = SparkStartup.catalogOperator.getEntityOption(entityname, Some(Entity.APPROX_COUNT_KEY)).get.getOrElse(Entity.APPROX_COUNT_KEY, "0").toInt
        SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.APPROX_COUNT_KEY, (apxCountOld + insertionSize).toString)


      } finally {
        lock.unlockWrite(stamp)
      }


      if (ninsertsvacuum > MAX_INSERTS_BEFORE_VACUUM || ninsertsvacuum % (MAX_INSERTS_BEFORE_VACUUM / 5) == 0 && getFeatureDataFast.isDefined && getFeatureDataFast.get.rdd.getNumPartitions > Entity.DEFAULT_MAX_PARTITIONS) {
        log.info("number of inserts necessitates now re-partitioning")

        if (schema().filter(_.storagehandler.engine.isInstanceOf[ParquetEngine]).nonEmpty) {
          //entity is partitionable
          vacuum()
        } else {
          //entity is not partitionable, increase number of max inserts to max value
          SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.MAX_INSERTS_VACUUMING, Int.MaxValue.toString)
        }
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Vacuums the entity, i.e., clean up operations for entity
    */
  def vacuum(): Unit = {
    val lock = ac.getEntityLock(entityname)
    val stamp = lock.writeLock()

    try {
      EntityPartitioner(this, ac.config.defaultNumberOfPartitions, attribute = Some(pk.name))
      SparkStartup.catalogOperator.updateEntityOption(entityname, Entity.N_INSERTS_VACUUMING, 0.toString)
    } finally {
      lock.unlock(stamp)
    }
  }


  /**
    * Deletes tuples that fit the predicates.
    *
    * @param predicates
    */
  def delete(predicates: Seq[Predicate]): Int = {
    var newData = getData().get

    predicates.foreach { predicate =>
      newData = newData.filter("NOT " + predicate.sqlString)
    }

    val handlers = newData.schema.fields
      .map(field => schema(Some(Seq(field.name)), fullSchema = false)).filterNot(_.isEmpty).map(_.head)
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

    markStale()

    val newCount = count

    (oldCount - newCount).toInt
  }


  /**
    * Returns all available indexes for the entity.
    *
    * @return
    */
  def indexes: Seq[Try[Index]] = {
    SparkStartup.catalogOperator.listIndexes(Some(entityname)).get.map(index => Index.load(index))
  }


  /**
    * Marks the data stale (e.g., if new data has been inserted to entity).
    */
  def markStale(): Unit = {
    mostRecentVersion.add(1)

    _schema = None
    _data.map(_.unpersist())
    _data = None

    indexes.map(_.map(_.markStale()))

    ac.entityLRUCache.invalidate(entityname)

    SparkStartup.catalogOperator.deleteEntityOption(entityname, Entity.COUNT_KEY)

    currentVersion = mostRecentVersion.value
  }


  /**
    * Checks if cached data is up to date, i.e. if version of local entity corresponds to global version of entity
    */
  private def checkVersions(): Unit = {
    if (currentVersion < mostRecentVersion.value) {

      _schema = None
      _data.map(_.unpersist())
      _data = None
      ac.entityLRUCache.invalidate(entityname)

      currentVersion = mostRecentVersion.value
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
      SparkStartup.catalogOperator.dropEntity(entityname)
    }
  }

  /**
    * Returns stored entity options.
    */
  private def options = SparkStartup.catalogOperator.getEntityOption(entityname)

  /**
    * Returns a map of properties to the entity. Useful for printing.
    *
    * @param options
    */
  def propertiesMap(options: Map[String, String] = Map()) = {
    val lb = ListBuffer[(String, String)]()

    lb.append("attributes" -> schema(fullSchema = false).map(field => field.name).mkString(","))
    lb.append("indexes" -> SparkStartup.catalogOperator.listIndexes(Some(entityname)).get.mkString(","))

    val apxCount = SparkStartup.catalogOperator.getEntityOption(entityname, Some(Entity.APPROX_COUNT_KEY)).get.get(Entity.APPROX_COUNT_KEY)
    if(apxCount.isDefined){
      lb.append("apxCount" -> apxCount.get)
    }

    try {
      lb.append("partitions" -> getFeatureDataFast.map(_.rdd.getNumPartitions.toString).getOrElse("none"))
    } catch {
      case e: Exception => log.warn("no partition information retrievable, possibly no data yet inserted")
    }

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
    schema(Some(Seq(attribute)), fullSchema = false).headOption.map(_.propertiesMap).getOrElse(Map())
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
  type AttributeName = AttributeNameHolder

  private val COUNT_KEY = "ntuples"
  private val APPROX_COUNT_KEY = "ntuplesapprox"

  private val N_INSERTS = "ninserts"
  private val N_INSERTS_VACUUMING = "ninsertsvac"
  private val MAX_INSERTS_VACUUMING = "maxinserts"
  private val DEFAULT_MAX_INSERTS_BEFORE_VACUUM = 100
  private val DEFAULT_MAX_PARTITIONS = 100

  /**
    * Check if entity exists. Note that this only checks the catalog; the entity may still exist in the file system.
    *
    * @param entityname name of entity
    * @return
    */
  def exists(entityname: EntityName)(implicit ac: AdamContext): Boolean = {
    val res = SparkStartup.catalogOperator.existsEntity(entityname)

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

      val reservedNames = creationAttributes.map(a => a.name -> AttributeNames.isNameReserved(a.name)).filter(_._2 == true)
      if (reservedNames.nonEmpty) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with field " + reservedNames.map(_._1).mkString + ", but name is reserved"))
      }

      if (creationAttributes.map(_.name).distinct.length != creationAttributes.length) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with duplicate fields; note that all attribute names have to be lower-case."))
      }

      val pk = new AttributeDefinition(AttributeNames.internalIdColumnName, TupleID.AdamTupleID)
      val attributes = creationAttributes.+:(pk)

      SparkStartup.catalogOperator.createEntity(entityname, attributes)
      SparkStartup.catalogOperator.updateEntityOption(entityname, COUNT_KEY, "0")
      SparkStartup.catalogOperator.updateEntityOption(entityname, APPROX_COUNT_KEY, "0")

      creationAttributes.groupBy(_.storagehandler).foreach {
        case (handler, handlerAttributes) =>
          val status = handler.create(entityname, handlerAttributes.+:(pk))
          if (status.isFailure) {
            throw new GeneralAdamException("failing on handler " + handler.name + ":" + status.failed.get)
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
        SparkStartup.catalogOperator.dropEntity(entityname, true)

        Failure(e)
    }
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list(implicit ac: AdamContext): Seq[EntityName] = SparkStartup.catalogOperator.listEntities().get


  /**
    * Loads an entity.
    *
    * @param entityname name of entity
    * @return
    */
  def load(entityname: EntityName, cache: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    val entity = ac.entityLRUCache.get(entityname)

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
      return Failure(EntityNotExistingException.withEntityname(entityname))
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
          return Failure(EntityNotExistingException.withEntityname(entityname))
        } else {
          return Success(null)
        }
      }

      Entity.load(entityname).get.drop()
      ac.entityLRUCache.invalidate(entityname)

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}