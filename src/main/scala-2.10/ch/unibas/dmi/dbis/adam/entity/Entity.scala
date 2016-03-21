package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.FieldTypes.{FieldType, LONGTYPE}
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.immutable.HashSet

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
//TODO: consider what to do if metadata is not filled, i.e. certain operations should not be done on the stack
case class Entity(entityname: EntityName, featureStorage: FeatureStorage, metadataStorage: Option[MetadataStorage]) {
  val log = Logger.getLogger(getClass.getName)

  private lazy val featureData = featureStorage.read(entityname).withColumnRenamed(FieldNames.featureColumnName, FieldNames.internFeatureColumnName)
  private lazy val metaData = if (metadataStorage.isDefined) {
    Some(metadataStorage.get.read(entityname))
  } else {
    None
  }

  /**
    * Returns number of elements in the entity (only feature storage is considered).
    *
    * @return
    */
  def count = featureStorage.count(entityname)

  /**
    * Gives preview of entity.
    *
    * @param k number of elements to show in preview
    * @return
    */
  def show(k: Int) = rdd.take(k)

  /**
    * Inserts data into the entity.
    *
    * @param insertion
    * @return
    */
  def insert(insertion: DataFrame): Boolean = {
    log.debug("inserting data into entity")

    val rows = insertion.rdd.zipWithUniqueId.map { case (r: Row, adamtwoid: Long) => Row.fromSeq(adamtwoid +: r.toSeq) }
    val insertionWithPK = SparkStartup.sqlContext
      .createDataFrame(
        rows, StructType(StructField(FieldNames.idColumnName, LongType, false) +: insertion.schema.fields))
      .withColumnRenamed(FieldNames.featureColumnName, FieldNames.internFeatureColumnName)

    featureStorage.write(entityname, insertionWithPK.select(FieldNames.idColumnName, FieldNames.internFeatureColumnName), SaveMode.Append)

    if (metadataStorage.isDefined) {
      log.debug("metadata storage is defined: inserting data also into metadata storage")
      metadataStorage.get.write(entityname, insertionWithPK.drop(FieldNames.internFeatureColumnName), SaveMode.Append)
    }

    true
  }

  /**
    *
    * @param filter
    * @return
    */
  def filter(filter: HashSet[Long]): RDD[Tuple] = {
    featureStorage.read(entityname, Option(filter)).rdd.map(r => r: Tuple)
  }

  def rdd = if (metaData.isDefined) {
    featureData.join(metaData.get).rdd
  } else {
    featureData.rdd
  }

  def getFeaturedata: DataFrame = featureData

  def hasMetadata: Boolean = metadataStorage.isDefined

  def getMetadata: Option[DataFrame] = metaData
}

object Entity {
  val log = Logger.getLogger(getClass.getName)

  type EntityName = String

  private val featureStorage = SparkStartup.featureStorage
  private val metadataStorage = SparkStartup.metadataStorage

  def exists(entityname: EntityName): Boolean = CatalogOperator.existsEntity(entityname)

  /**
    * Creates an entity.
    *
    * @param entityname
    * @param fields if fields is specified, in the metadata storage a table is created with these names, specify fields
    *               as key = name, value = SQL type
    * @return
    */
  def create(entityname: EntityName, fields: Option[Map[String, FieldType]] = None): Entity = {
    if (exists(entityname)) {
      throw new EntityExistingException()
    }

    featureStorage.create(entityname)

    if (fields.isDefined) {
      //TODO: add optional information, e.g. PK to creation of metadata
      val fieldsWithPK = fields.get + (FieldNames.idColumnName -> LONGTYPE)
      metadataStorage.create(entityname, fieldsWithPK.mapValues(_.datatype))
      CatalogOperator.createEntity(entityname, true)
      Entity(entityname, featureStorage, Option(metadataStorage))
    } else {
      CatalogOperator.createEntity(entityname, false)
      Entity(entityname, featureStorage, None)
    }
  }

  /**
    * Drops an entity.
    *
    * @param entityname
    * @param ifExists
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false): Boolean = {
    if (!exists(entityname)) {
      if (!ifExists) {
        throw new EntityNotExistingException()
      } else {
        return false
      }
    }

    Index.dropAll(entityname)

    featureStorage.drop(entityname)

    if (CatalogOperator.hasEntityMetadata(entityname)) {
      metadataStorage.drop(entityname)
    }

    CatalogOperator.dropEntity(entityname, ifExists)
  }

  /**
    * Inserts data into an entity.
    *
    * @param entityname
    * @param insertion data frame containing all columns (of both the feature storage and the metadata storage);
    *                  note that you should name the feature column as ("feature").
    * @return
    */
  def insertData(entityname: EntityName, insertion: DataFrame): Boolean = {
    load(entityname).insert(insertion)
  }

  /**
    * Loads an entity.
    *
    * @param entityname
    * @return
    */
  def load(entityname: EntityName): Entity = {
    if (!exists(entityname)) {
      throw new EntityNotExistingException()
    }

    val entityMetadataStorage = if (CatalogOperator.hasEntityMetadata(entityname)) {
      Option(metadataStorage)
    } else {
      None
    }

    Entity(entityname, featureStorage, entityMetadataStorage)
  }

  /**
    * Returns number of tuples in entity (only feature storage is considered).
    *
    * @param entityname
    * @return the number of tuples in the entity
    */
  def countTuples(entityname: EntityName): Int = {
    if (!exists(entityname)) {
      throw new EntityNotExistingException()
    }

    featureStorage.count(entityname)
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list(): List[EntityName] = CatalogOperator.listEntities()
}