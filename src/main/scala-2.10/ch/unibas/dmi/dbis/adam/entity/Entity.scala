package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.{EntityCreationException, EntityExistingException, EntityNotExistingException}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{Row, DataFrame, SaveMode}

import scala.collection.immutable.HashSet
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
case class Entity(entityname: EntityName, featureStorage: FeatureStorage, metadataStorage: MetadataStorage) {
  private lazy val featureData = featureStorage.read(entityname)
  private lazy val metaData = metadataStorage.read(entityname)

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
  def show(k: Int) = featureData.join(metaData).take(k)

  /**
    *
    * @param filter
    * @return
    */
  def filter(filter: HashSet[Long]): RDD[Tuple] = {
    featureStorage.read(entityname, Option(filter)).rdd.map(r => r: Tuple)
  }

  def rdd = featureData.join(metaData).rdd

  def getFeaturedata: DataFrame = featureData

  def getMetadata: DataFrame = metaData
}

object Entity {
  type EntityName = String

  private val featureStorage = SparkStartup.featureStorage
  private val metadataStorage = SparkStartup.metadataStorage

  def existsEntity(entityname: EntityName): Boolean = CatalogOperator.existsEntity(entityname)

  /**
    * Creates an entity.
    *
    * @param entityname
    * @param fields if fields is specified, in the metadata storage a table is created with these names, specify fields
    *               as key = name, value = SQL type
    * @return
    */
  def create(entityname: EntityName, fields: Option[Map[String, String]] = None): Entity = {
    if (existsEntity(entityname)) {
      throw new EntityExistingException()
    }

    val future = Future {
      featureStorage.create(entityname)
      metadataStorage.create(entityname, fields)
    }

    future onFailure { case t => new EntityCreationException() }
    future onSuccess { case t => CatalogOperator.createEntity(entityname) }
    Await.ready(future, Duration.Inf)

    Entity(entityname, featureStorage, metadataStorage)
  }

  /**
    * Drops an entity.
    *
    * @param entityname
    * @param ifExists
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false): Boolean = {
    if (!existsEntity(entityname)) {
      if (!ifExists) {
        throw new EntityNotExistingException()
      } else {
        return false
      }
    }

    val indexes = CatalogOperator.listIndexes(entityname)

    featureStorage.drop(entityname) && metadataStorage.drop(entityname) && CatalogOperator.dropEntity(entityname, ifExists)
  }

  /**
    * Inserts data into an entity.
    *
    * @param entityname
    * @param insertion data frame containing all columns (of both the feature storage and the metadata storage);
    *                  note that you should name the feature column as specified in the feature storage class ("feature").
    * @return
    */
  def insertData(entityname: EntityName, insertion: DataFrame): Boolean = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    val rows = insertion.rdd.zipWithUniqueId.map { case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }
    val insertionWithPK = SparkStartup.sqlContext.createDataFrame(
      rows, StructType(StructField(featureStorage.idColumnName, LongType, false) +: insertion.schema.fields))

    featureStorage.write(entityname, insertionWithPK.select(featureStorage.idColumnName, featureStorage.featureColumnName), SaveMode.Append)
    metadataStorage.write(entityname, insertionWithPK.drop(featureStorage.featureColumnName), SaveMode.Append)
  }

  /**
    * Loads an entity.
    *
    * @param entityname
    * @return
    */
  def load(entityname: EntityName): Entity = {
    if (!existsEntity(entityname)) {
      throw new EntityNotExistingException()
    }

    Entity(entityname, featureStorage, metadataStorage)

  }

  /**
    * Returns number of tuples in entity (only feature storage is considered).
    *
    * @param entityname
    * @return the number of tuples in the entity
    */
  def countTuples(entityname: EntityName): Int = {
    if (!existsEntity(entityname)) {
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