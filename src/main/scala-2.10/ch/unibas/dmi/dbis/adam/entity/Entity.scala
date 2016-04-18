package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
case class Entity(entityname: EntityName, featureStorage: FeatureStorage, metadataStorage: Option[MetadataStorage])(implicit ac: AdamContext) {
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
  def count: Int = {
    if (tupleCount == -1) {
      tupleCount = featureStorage.count(entityname)
    }

    tupleCount
  }

  private var tupleCount = -1

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
  def insert(insertion: DataFrame)(implicit ac: AdamContext): Try[Void] = {
    log.debug("inserting data into entity")

    val dataDimensionality = insertion.first.getAs[FeatureVectorWrapper](FieldNames.featureColumnName).vector.size

    val entityDimensionality = CatalogOperator.getDimensionality(entityname)
    if (entityDimensionality.isDefined && dataDimensionality != entityDimensionality.get) {
      log.warn("new data has not same dimensionality as existing data")
    } else {
      CatalogOperator.updateDimensionality(entityname, dataDimensionality)
    }

    val rdd = insertion.rdd.zipWithUniqueId.map { case (r: Row, adamtwoid: Long) => Row.fromSeq(adamtwoid +: r.toSeq) }
    val insertionWithPK = ac.sqlContext
      .createDataFrame(
        rdd, StructType(StructField(FieldNames.idColumnName, LongType, false) +: insertion.schema.fields))
      .withColumnRenamed(FieldNames.featureColumnName, FieldNames.internFeatureColumnName)

    featureStorage.write(entityname, insertionWithPK.select(FieldNames.idColumnName, FieldNames.internFeatureColumnName), SaveMode.Append)

    if (metadataStorage.isDefined) {
      log.debug("metadata storage is defined: inserting data also into metadata storage")
      metadataStorage.get.write(entityname, insertionWithPK.drop(FieldNames.internFeatureColumnName), SaveMode.Append)
    }

    if (!CatalogOperator.listIndexes(entityname).isEmpty) {
      log.warn("new data inserted, but indexes are not updated; please re-create index")
    }

    //reset count
    tupleCount = -1

    Success(null)
  }

  /**
    * Returns a map of properties to the entity. Useful for printing.
    */
  def getEntityProperties(): Map[String, String] = {
    val lb = ListBuffer[(String, String)]()


    lb.append("hasMetadata" -> hasMetadata.toString)
    lb.append("dimensionality" -> CatalogOperator.getDimensionality(entityname).getOrElse(-1).toString)
    lb.append("schema" -> schema.map(field => field.name + "(" + field.dataType.simpleString + ")").mkString(","))
    lb.append("indexes" -> CatalogOperator.listIndexes(entityname).mkString(", "))

    lb.toMap
  }

  /**
    *
    * @param filter
    * @return
    */
  def filter(filter: Set[Long]): RDD[Tuple] = {
    featureStorage.read(entityname, Option(filter)).rdd.map(r => r: Tuple)
  }

  /**
    *
    * @return
    */
  def schema = if (metaData.isDefined) {
    featureData.join(metaData.get).drop(FieldNames.idColumnName).schema
  } else {
    featureData.drop(FieldNames.idColumnName).schema
  }

  /**
    *
    * @return
    */
  def rdd = if (metaData.isDefined) {
    featureData.join(metaData.get).rdd
  } else {
    featureData.rdd
  }

  /**
    *
    * @return
    */
  def getFeaturedata: DataFrame = featureData

  /**
    *
    * @return
    */
  def hasMetadata: Boolean = metadataStorage.isDefined

  /**
    *
    * @return
    */
  def getMetadata: Option[DataFrame] = metaData

  /**
    *
    * @param query
    * @return
    */
  def isQueryConform(query: NearestNeighbourQuery): Boolean = {
    val entityDimensionality = CatalogOperator.getDimensionality(entityname)

    if (entityDimensionality.isDefined && query.q.size != entityDimensionality.get) {
      return false
    } else {
      return true
    }
  }
}

/**
  * Field definition for creating new entity.
  *
  * @param fieldtype
  * @param pk
  * @param unique
  * @param indexed
  */
case class FieldDefinition(fieldtype: FieldType, pk: Boolean = false, unique: Boolean = false, indexed: Boolean = false)


object Entity {
  type EntityName = EntityNameHolder
}