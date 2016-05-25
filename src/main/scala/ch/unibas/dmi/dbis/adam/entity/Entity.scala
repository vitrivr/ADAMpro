package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.FieldTypes._
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
case class Entity(val entityname: EntityName)(@transient implicit val ac: AdamContext) extends Logging {
  def featureData: Option[DataFrame] = {
    if (featurePath.isDefined) {
      val data = Entity.featureStorage.read(featurePath.get)

      if (data.isSuccess) {
        Some(data.get)
      } else {
        None
      }
    } else {
      None
    }
  }

  private[adam] def getIndexableFeature(column: String) = {
    if (featureData.isDefined) {
      featureData.get.select(col(pk.name), col(column))
    } else {
      val structFields = StructType(Seq(
        StructField(pk.name, pk.fieldtype.datatype),
        StructField(column, FieldTypes.FEATURETYPE.datatype)
      ))
      sqlContext.createDataFrame(sc.emptyRDD[Row], structFields)
    }
  }

  def metaData: Option[DataFrame] = {
    if (metadataPath.isDefined) {
      val data = Entity.metadataStorage.read(metadataPath.get)

      if (data.isSuccess) {
        Some(data.get)
      } else {
        None
      }

    } else {
      None
    }
  }

  /**
    * Gets the data.
    *
    * @return
    */
  def data = {
    if (featureData.isDefined && metaData.isDefined) {
      featureData.get.join(metaData.get, pk.name)
    } else if (featureData.isDefined) {
      featureData.get
    } else if (metaData.isDefined) {
      metaData.get
    } else {
      val structFields = schema.map {
        field => StructField(field.name, field.fieldtype.datatype)
      }

      sqlContext.createDataFrame(sc.emptyRDD[Row], StructType(structFields))
    }
  }

  /**
    * Gets the primary key.
    *
    * @return
    */
  val pk = CatalogOperator.getEntityPK(entityname)


  /**
    * Gets path of the entity.
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

  private[entity] def metadataPath: Option[String] = {
    val path = CatalogOperator.getEntityMetadataPath(entityname)
    if (path != null && path.length > 0) {
      Some(path)
    } else {
      None
    }
  }


  /**
    *
    * @return
    */
  lazy val indexes: Seq[Try[Index]] = {
    CatalogOperator.listIndexes(entityname).map(index => Index.load(index._1))
  }


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

  private var tupleCount: Option[Long] = None


  /**
    * Gives preview of entity.
    *
    * @param k number of elements to show in preview
    * @return
    */
  def show(k: Int) = data.limit(k)

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

      //TODO: check schema

      val featureFieldNames = insertion.schema.fields.filter(_.dataType == new FeatureVectorWrapperUDT).map(_.name)
      val newFeaturePath = Entity.featureStorage.write(entityname, insertion.select(pk.name, featureFieldNames.toSeq: _*), SaveMode.Append, featurePath, true)

      if (featurePath.isEmpty) {
        if (newFeaturePath.isSuccess) {
          CatalogOperator.updateEntityFeaturePath(entityname, newFeaturePath.get)
        }
      }

      if (newFeaturePath.isFailure) {
        return Failure(newFeaturePath.failed.get)
      }

      val metadataFieldNames = insertion.schema.fields.filterNot(_.dataType == new FeatureVectorWrapperUDT).map(_.name)
      if (metadataPath.isDefined) {
        log.debug("metadata storage is defined: inserting data also into metadata storage")
        Entity.metadataStorage.write(entityname, insertion.select(pk.name, metadataFieldNames.filterNot(x => x == pk.name).toSeq: _*), SaveMode.Append)
      }

      if (CatalogOperator.listIndexes(entityname).nonEmpty) {
        log.warn("new data inserted, but indexes are not updated; please re-create index")
        CatalogOperator.updateIndexesToStale(entityname)
      }

      //reset count
      tupleCount = None

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Returns a map of properties to the entity. Useful for printing.
    */
  def properties: Map[String, String] = {
    val lb = ListBuffer[(String, String)]()

    lb.append("schema" -> CatalogOperator.getFields(entityname).map(field => field.name + "(" + field.fieldtype.name + ")").mkString(","))
    lb.append("indexes" -> CatalogOperator.listIndexes(entityname).mkString(", "))

    lb.toMap
  }

  /**
    *
    * @return
    */
  def schema: Seq[FieldDefinition] = CatalogOperator.getFields(entityname)

  /**
    *
    * @param query
    * @return
    */
  def isQueryConform(query: NearestNeighbourQuery): Boolean = {
    //TODO: check dimensionality of field and compare to dimensionality of query
    true
  }
}

object Entity extends Logging {
  type EntityName = EntityNameHolder

  private val featureStorage = SparkStartup.featureStorage
  private val metadataStorage = SparkStartup.metadataStorage

  def exists(entityname: EntityName): Boolean = CatalogOperator.existsEntity(entityname)

  private val lock = new Object() //TODO: make entities singleton? lock on entity?

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
          return Failure(EntityNotProperlyDefinedException(Some("Too many auto fields defined.")))
        } else if (fields.count(_.fieldtype == AUTOTYPE) > 0 && !fields.filter(_.pk).forall(_.fieldtype == AUTOTYPE)) {
          return Failure(EntityNotProperlyDefinedException(Some("Auto type only allowed in primary key.")))
        }


        val pk = fields.filter(_.pk).head

        //perform
        val featurePath = if (!fields.filter(_.fieldtype == FEATURETYPE).isEmpty) {
          val featureFields = fields.filter(_.fieldtype == FEATURETYPE)

          val path = featureStorage.create(entityname, featureFields)

          if(path.isFailure){
            throw path.failed.get
          }

          path.get
        } else {
          None
        }

        val metadataPath = if (!fields.filterNot(_.fieldtype == FEATURETYPE).filterNot(_.pk).isEmpty) {
          val metadataFields = fields.filterNot(_.fieldtype == FEATURETYPE)
          val path = metadataStorage.create(entityname, metadataFields)

          if(path.isFailure){
            throw path.failed.get
          }

          path.get
        } else {
          None
        }

        CatalogOperator.createEntity(entityname, featurePath, metadataPath, fields, !fields.filterNot(_.fieldtype == FEATURETYPE).filterNot(_.pk).isEmpty)

        Success(Entity(entityname)(ac))
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

      Index.dropAll(entityname)

      featureStorage.drop(CatalogOperator.getEntityFeaturePath(entityname))

      if (CatalogOperator.hasEntityMetadata(entityname)) {
        metadataStorage.drop(CatalogOperator.getEntityMetadataPath(entityname))
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
    * @param entityname
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
    * @param entity
    * @param nPartitions
    * @param join
    * @param cols
    * @param option
    * @return
    */
  def repartition(entity: Entity, nPartitions: Int, join: Option[DataFrame], cols: Option[Seq[String]], option: PartitionMode.Value)(implicit ac: AdamContext): Try[Entity] = {
    if (entity.featurePath.isEmpty) {
      return Failure(new GeneralAdamException("no feature data available for performing repartitioning"))
    }

    var data = entity.data

    //TODO: possibly add own partitioner
    //data.map(r => (r.getAs(cols.get.head), r)).partitionBy(new HashPartitioner())

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

    option match {
      case PartitionMode.REPLACE_EXISTING =>
        val oldPath = entity.featurePath.get

        var newPath = ""

        do {
          newPath = entity.featurePath + "-rep" + Random.nextInt
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
  def list(): Seq[EntityName] = CatalogOperator.listEntities()
}