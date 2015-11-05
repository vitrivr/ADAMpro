package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.cache.RDDCache
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.{EntityCreationException, EntityNotExistingException}
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

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
case class Entity(entityname : EntityName, featureStorage : FeatureStorage, metadataStorage: MetadataStorage) {
  private lazy val featureData = featureStorage.read(entityname)
  private lazy val metaData = metadataStorage.read(entityname)
  
  def count = featureData.count()
  def show() = featureData.collect()
  def show(n : Int) = featureData.take(n)
  
  def featuresForKeys(filter: HashSet[Long]): RDD[Tuple] = {
    featureStorage.read(entityname, Option(filter)).rdd.map(r => r :Tuple)
  }
  def featuresRDD = featureData.rdd
  def featuresTuples = featureData.rdd.map(row => (row : Tuple))
  def getFeaturedata: DataFrame = featureData
  
  def getMetadata : DataFrame = metaData
}

object Entity {
  type EntityName = String

  private val sqlContext = SparkStartup.sqlContext
  private val featureStorage = SparkStartup.featureStorage
  private val metadataStorage = SparkStartup.metadataStorage


  def existsEntity(entityname : EntityName) : Boolean = CatalogOperator.existsEntity(entityname)

  /**
   *
   * @param entityname
   * @param schema
   * @return
   */
  def createEntity(entityname : EntityName, schema : StructType) : Entity = {
    val fields = schema.fields

    require(fields.filter(_.name == "feature").length <= 1)

    CatalogOperator.createEntity(entityname)

    val featureSchema = StructType(
      List(
        StructField("__adam_id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )
    val featureData = sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], featureSchema)


    val metadataSchema = StructType(
      List(StructField("__adam_id", LongType, false)) ::: fields.filterNot(_.name == "feature").toList
    )
    val metadataData = sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], metadataSchema)


    val future = Future {
      featureStorage.write(entityname, featureData, SaveMode.ErrorIfExists)
      metadataStorage.write(entityname, metadataData, SaveMode.ErrorIfExists)
    }
    future onFailure {case t => new EntityCreationException()}

    Await.ready(future, Duration.Inf)

    Entity(entityname, featureStorage, metadataStorage)
  }

  /**
   *
   * @param entityname
   * @param ifExists
   */
  def dropEntity(entityname : EntityName, ifExists : Boolean = false) : Unit = {
    val indexes = CatalogOperator.getIndexes(entityname)
    CatalogOperator.dropEntity(entityname, ifExists)

    featureStorage.drop(entityname)
  }

  /**
   *
   * @param entityname
   * @return
   */
  def insertData(entityname : EntityName, insertion: DataFrame): Unit ={
    if(!existsEntity(entityname)){
      throw new EntityNotExistingException()
    }

    val future = Future {
      featureStorage.write(entityname, insertion, SaveMode.Append)
    }

    Await.ready(future, Duration.Inf)
  }

  /**
   *
   * @param entityname
   * @return
   */
  def retrieveEntity(entityname : EntityName) : Entity = {
    if(!existsEntity(entityname)){
      throw new EntityNotExistingException()
    }

    if(RDDCache.containsTable(entityname)){
      RDDCache.getTable(entityname)
    } else {
      Entity(entityname, featureStorage, metadataStorage)
    }
  }

  /**
   *
   * @param entityname
   * @return
   */
  def getCacheable(entityname : EntityName) : CacheableEntity = {
    val entity = Entity.retrieveEntity(entityname)

    entity.featuresTuples
      .repartition(Startup.config.partitions)
      .setName(entityname).persist(StorageLevel.MEMORY_AND_DISK)
      .collect()

    CacheableEntity(entity)
  }
}