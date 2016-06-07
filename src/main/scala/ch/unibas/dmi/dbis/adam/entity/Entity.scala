package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes._
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, EntityNotProperlyDefinedException, GeneralAdamException}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.storage.partition.PartitionMode
import org.apache.spark.Logging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
case class Entity(val entityname: EntityName)(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  var _featureData: Option[DataFrame] = None
  var _metaData: Option[DataFrame] = None
  var _join: Option[DataFrame] = None

  //TODO: make entities singleton? lock on entity?


  /**
    * Gets path of feature data.
    *
    * @return
    */
  private[entity] def featurePath: Option[String] = {
    val path = CatalogOperator.getEntityFeaturePath(entityname)
    if (path != null && path.length > 0) {
      Some(path)
    } else {
      None
    }
  }

  /**
    * Gets feature data.
    *
    * @return
    */
  def featureData: Option[DataFrame] = {
    if (_featureData.isEmpty) {
      if (featurePath.isDefined) {
        val data = Entity.featureStorage.read(featurePath.get)

        if (data.isSuccess) {
          _featureData = Some(data.get)
        }
      }
    }

    _featureData
  }

  /**
    * Gets feature column and pk column; use this for indexing purposes.
    *
    * @param attribute attribute that is indexed
    * @return
    */
  private[adam] def getIndexableFeature(attribute: String) = {
    if (featureData.isDefined) {
      featureData.get.select(col(pk.name), col(attribute))
    } else {
      val structFields = StructType(Seq(
        StructField(pk.name, pk.fieldtype.datatype),
        StructField(attribute, FieldTypes.FEATURETYPE.datatype)
      ))
      sqlContext.createDataFrame(sc.emptyRDD[Row], structFields)
    }
  }

  /**
    * Gets path of metadata.
    *
    * @return
    */
  private[entity] def metadataPath: Option[String] = {
    val path = CatalogOperator.getEntityMetadataPath(entityname)
    if (path != null && path.length > 0) {
      Some(path)
    } else {
      None
    }
  }

  /**
    * Gets feature data.
    *
    * @return
    */
  def metaData: Option[DataFrame] = {
    if (_metaData.isEmpty) {
      if (metadataPath.isDefined) {
        val data = Entity.metadataStorage.read(metadataPath.get)

        if (data.isSuccess) {
          _metaData = Some(data.get)
        }
      }
    }

    _metaData
  }

  /**
    * Gets the full entity data.
    *
    * @return
    */
  def data: Option[DataFrame] = {
    if (_join.isEmpty) {
      if (featureData.isDefined && metaData.isDefined) {
        _join = Some(featureData.get.join(metaData.get, pk.name))
      } else if (featureData.isDefined) {
        _join = Some(featureData.get)
      } else if (metaData.isDefined) {
        _join = Some(metaData.get)
      }
    }

    _join
  }

  /**
    * Marks the data stale (e.g., if new data has been inserted to entity).
    */
  private def markStale(): Unit = {
    _featureData = None
    _metaData = None
    _join = None
    tupleCount = None
  }

  /**
    * Gets the primary key.
    *
    * @return
    */
  val pk = CatalogOperator.getEntityPK(entityname)


  /**
    *
    * @return
    */
  lazy val indexes: Seq[Try[Index]] = {
    CatalogOperator.listIndexes(entityname).map(index => Index.load(index._1))
  }


  /**
    * Returns the weight set to the entity scan. The weight is used at query time to choose which query path to choose.
    */
  def scanweight(attribute: String) = CatalogOperator.getEntityWeight(entityname, attribute)

  /**
    *
    * @param weight new weights to set to entity scan
    * @return
    */
  def setScanWeight(attribute: String, weight: Option[Float] = None): Boolean = {
    CatalogOperator.updateEntityWeight(entityname, attribute, weight)
  }

  private var tupleCount: Option[Long] = None


  /**
    * Returns number of elements in the entity (only the feature storage is considered for this).
    *
    * @return
    */
  def count: Long = {
    //TODO: possibly switch to metadata storage?
    if (tupleCount.isEmpty) {
      tupleCount = if (featurePath.isDefined) {
        Some(Entity.featureStorage.count(featurePath.get).get)
      } else {
        Some(0)
      }
    }

    tupleCount.get
  }


  /**
    * Gives preview of entity.
    *
    * @param k number of elements to show in preview
    * @return
    */
  def show(k: Int): Option[DataFrame] = {
    if (data.isDefined) {
      Some(data.get.limit(k))
    } else {
      None
    }
  }

  /**
    * Inserts data into the entity.
    *
    * @param data
    * @param ignoreChecks
    * @return
    */
  def insert(data: DataFrame, ignoreChecks: Boolean = false): Try[Void] = {
    log.debug("inserting data into entity")

    try {
      val insertion =
        if (pk.fieldtype.equals(FieldTypes.AUTOTYPE)) {
          if (data.schema.fieldNames.contains(pk.name)) {
            return Failure(new GeneralAdamException("the field " + pk.name + " has been specified as auto and should therefore not be provided"))
          }

          val rdd = data.rdd.zipWithUniqueId.map { case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }
          ac.sqlContext
            .createDataFrame(
              rdd, StructType(StructField(pk.name, pk.fieldtype.datatype, false) +: data.schema.fields))
        } else {
          data
        }

      //TODO: check that schema of data inserted and schema of entity are equal

      val featureFieldNames = insertion.schema.fields.filter(_.dataType.isInstanceOf[FeatureVectorWrapperUDT]).map(_.name)
      val newFeaturePath = Entity.featureStorage.write(entityname, insertion.select(pk.name, featureFieldNames.toSeq: _*), SaveMode.Append, featurePath, true)

      if (featurePath.isEmpty) {
        if (newFeaturePath.isSuccess) {
          CatalogOperator.updateEntityFeaturePath(entityname, newFeaturePath.get)
        }
      }

      if (newFeaturePath.isFailure) {
        return Failure(newFeaturePath.failed.get)
      }

      val metadataFieldNames = insertion.schema.fields.filterNot(_.dataType.isInstanceOf[FeatureVectorWrapperUDT]).map(_.name)
      if (metadataPath.isDefined) {
        log.debug("metadata storage is defined: inserting data also into metadata storage")
        Entity.metadataStorage.write(entityname, insertion.select(pk.name, metadataFieldNames.filterNot(x => x == pk.name).toSeq: _*), SaveMode.Append)
      }

      if (CatalogOperator.listIndexes(entityname).nonEmpty) {
        log.warn("new data inserted, but indexes are not updated; please re-create index")
        CatalogOperator.updateIndexesToStale(entityname)
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    } finally {
      //mark entity stale
      markStale()
    }
  }

  /**
    * Returns a map of properties to the entity. Useful for printing.
    */
  def properties: Map[String, String] = {
    val lb = ListBuffer[(String, String)]()

    lb.append("scanweight" -> schema.filter(_.fieldtype == FieldTypes.FEATURETYPE).map(field => field.name -> scanweight(field.name)).map { case (name, weight) => name + "(" + weight + ")" }.mkString(","))
    lb.append("schema" -> CatalogOperator.getFields(entityname).map(field => field.name + "(" + field.fieldtype.name + ")").mkString(","))
    lb.append("indexes" -> CatalogOperator.listIndexes(entityname).mkString(", "))

    lb.toMap
  }

  /**
    * Schema of the entity.
    *
    * @return
    */
  def schema: Seq[AttributeDefinition] = CatalogOperator.getFields(entityname)


  /**
    * Checks whether query is conform to entity.
    *
    * @param query the query to be performed
    * @return
    */
  def isQueryConform(query: NearestNeighbourQuery): Boolean = {
    if (featureData.isDefined) {
      try {
        val length = featureLength(query.column)
        if (length != query.q.length) {
          log.error("expected vector of length " + length + ", but received " + query.q.length)
        }

        length == query.q.length
      } catch {
        case e: Exception => log.error("query not conform with entity", e)
          false
      }
    } else {
      false
    }
  }

  lazy val featureLength = schema.filter(_.fieldtype == FEATURETYPE).map(attribute => attribute.name -> featureData.get.first().getAs[FeatureVectorWrapper](attribute.name).vector.length).toMap
}

object Entity extends Logging {
  type EntityName = EntityNameHolder

  private val featureStorage = SparkStartup.featureStorage
  private val metadataStorage = SparkStartup.metadataStorage

  def exists(entityname: EntityName): Boolean = CatalogOperator.existsEntity(entityname)

  /**
    * Creates an entity.
    *
    * @param entityname  name of the entity
    * @param attributes  attributes of entity
    * @param ifNotExists if set to true and the entity exists, the entity is just returned; otherwise an error is thrown
    * @return
    */
  def create(entityname: EntityName, attributes: Seq[AttributeDefinition], ifNotExists: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    try {
      //checks
      if (exists(entityname)) {
        if (!ifNotExists) {
          return Failure(EntityExistingException())
        } else {
          return load(entityname)
        }
      }

      if (attributes.isEmpty) {
        return Failure(EntityNotProperlyDefinedException("Entity " + entityname + " will have no attributes"))
      }

      FieldNames.reservedNames.foreach { reservedName =>
        if (attributes.contains(reservedName)) {
          return Failure(EntityNotProperlyDefinedException("Entity defined with field " + reservedName + ", but name is reserved"))
        }
      }

      if (attributes.map(_.name).distinct.length != attributes.length) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with duplicate fields."))
      }

      val allowedPkTypes = Seq(INTTYPE, LONGTYPE, STRINGTYPE, AUTOTYPE)

      if (attributes.count(_.pk) > 1) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with more than one primary key"))
      } else if (attributes.filter(_.pk).isEmpty) {
        return Failure(EntityNotProperlyDefinedException("Entity defined has no primary key."))
      } else if (!attributes.filter(_.pk).forall(field => allowedPkTypes.contains(field.fieldtype))) {
        return Failure(EntityNotProperlyDefinedException("Entity defined needs a " + allowedPkTypes.map(_.name).mkString(", ") + " primary key"))
      }

      if (attributes.count(_.fieldtype == AUTOTYPE) > 1) {
        return Failure(EntityNotProperlyDefinedException("Too many auto attributes defined."))
      } else if (attributes.count(_.fieldtype == AUTOTYPE) > 0 && !attributes.filter(_.pk).forall(_.fieldtype == AUTOTYPE)) {
        return Failure(EntityNotProperlyDefinedException("Auto type only allowed in primary key."))
      }


      val pk = attributes.filter(_.pk).head

      //perform
      val featurePath = if (!attributes.filter(_.fieldtype == FEATURETYPE).isEmpty) {
        val featureFields = attributes.filter(_.fieldtype == FEATURETYPE)

        val path = featureStorage.create(entityname, featureFields)

        if (path.isFailure) {
          throw path.failed.get
        }

        path.get
      } else {
        None
      }

      val metadataPath = if (!attributes.filterNot(_.fieldtype == FEATURETYPE).filterNot(_.pk).isEmpty) {
        val metadataFields = attributes.filterNot(_.fieldtype == FEATURETYPE)
        val path = metadataStorage.create(entityname, metadataFields)

        if (path.isFailure) {
          throw path.failed.get
        }

        path.get
      } else {
        None
      }

      CatalogOperator.createEntity(entityname, featurePath, metadataPath, attributes, !attributes.filterNot(_.fieldtype == FEATURETYPE).filterNot(_.pk).isEmpty)

      Success(Entity(entityname)(ac))
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
    * @param entityname name of entity
    * @param ifExists   if set to true, no error is raised if entity does not exist
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    if (!exists(entityname)) {
      if (!ifExists) {
        return Failure(EntityNotExistingException())
      } else {
        return Success(null)
      }
    }

    Index.dropAll(entityname)

    featureStorage.drop(CatalogOperator.getEntityFeaturePath(entityname))

    if (CatalogOperator.hasEntityMetadata(entityname)) {
      metadataStorage.drop(CatalogOperator.getEntityMetadataPath(entityname))
    }

    CatalogOperator.dropEntity(entityname, ifExists)
    Success(null)
  }

  /**
    * Loads an entity.
    *
    * @param entityname name of entity
    * @return
    */
  def load(entityname: EntityName, cache: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException("Entity " + entityname + " is not existing."))
    }

    val entity = EntityLRUCache.get(entityname)

    if (cache) {
      if (entity.get.featureData.isDefined) {
        entity.get.featureData.get.rdd.setName(entityname + "_feature").cache()
      }
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
    * @param entityname name of entity
    * @return
    */
  private[entity] def loadEntityMetaData(entityname: EntityName)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException())
    }

    try {
      val pk = CatalogOperator.getEntityPK(entityname)

      Success(Entity(entityname)(ac))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Repartition data.
    *
    * @param entity      entity
    * @param nPartitions number of partitions
    * @param join        other dataframes to join on, on which the partitioning is performed
    * @param cols        columns to partition on, if not specified the primary key is used
    * @param mode        partition mode
    * @return
    */
  def repartition(entity: Entity, nPartitions: Int, join: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Entity] = {
    if (entity.featurePath.isEmpty) {
      return Failure(new GeneralAdamException("no feature data available for performing repartitioning"))
    }

    var data = entity.data.get

    //TODO: possibly add own partitioner
    //data.map(r => (r.getAs[Any](cols.get.head), r)).partitionBy(new HashPartitioner())

    if (join.isDefined) {
      data = data.join(join.get, entity.pk.name)
    }

    data = if (cols.isDefined) {
      val entityColNames = data.schema.map(_.name)
      if (!cols.get.forall(name => entityColNames.contains(name))) {
        Failure(throw new GeneralAdamException("one of the columns " + cols.mkString(",") + " is not existing in entity " + entity.entityname + entityColNames.mkString("(", ",", ")")))
      }

      data.repartition(nPartitions, cols.get.map(data(_)): _*)
    } else {
      data.repartition(nPartitions, data(entity.pk.name))
    }

    data = data.select(entity.featureData.get.columns.map(col): _*)

    mode match {
      case PartitionMode.REPLACE_EXISTING =>
        val oldPath = entity.featurePath.get

        var newPath = ""

        do {
          newPath = oldPath + "-rep" + Random.nextInt
        } while (SparkStartup.featureStorage.exists(newPath).get)

        featureStorage.write(entity.entityname, data, SaveMode.ErrorIfExists, Some(newPath))
        CatalogOperator.updateEntityFeaturePath(entity.entityname, newPath)
        featureStorage.drop(oldPath)

        return Success(entity)

      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list: Seq[EntityName] = CatalogOperator.listEntities()
}