package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.components.{FeatureStorage, MetadataStorage}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
case class Entity(val entityname: EntityName, val pk : String, private val featureStorage: FeatureStorage, private val metadataStorage: Option[MetadataStorage]) {
  @transient val log = Logger.getLogger(getClass.getName)

  private def featureData(implicit ac: AdamContext) = featureStorage.read(entityname).get
  private def metaData = if (metadataStorage.isDefined) {
    Some(metadataStorage.get.read(entityname))
  } else {
    None
  }

  /**
    * Returns number of elements in the entity (only feature storage is considered).
    *
    * @return
    */
  def count(implicit ac: AdamContext): Long = {
    if (tupleCount == -1) {
      tupleCount = featureStorage.count(entityname)
    }

    tupleCount
  }

  private var tupleCount : Long = -1

  /**
    * Gives preview of entity.
    *
    * @param k number of elements to show in preview
    * @return
    */
  def show(k: Int)(implicit ac: AdamContext) = rdd.take(k)

  /**
    * Inserts data into the entity.
    *
    * @param insertion
    * @param ignoreChecks
    * @return
    */
  def insert(insertion: DataFrame, ignoreChecks: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    log.debug("inserting data into entity")

   //TODO: check dimensionality is correct
    //TODO: if PK not set in insertion, auto-increment?

    val featureFieldNames = insertion.schema.fields.filter(_.dataType == new FeatureVectorWrapperUDT).map(_.name)
    featureStorage.write(entityname, pk, insertion.select(pk, featureFieldNames.toSeq : _*), SaveMode.Append)

    val metadataFieldNames = insertion.schema.fields.filterNot(_.dataType == new FeatureVectorWrapperUDT).map(_.name)
    if (metadataStorage.isDefined) {
      log.debug("metadata storage is defined: inserting data also into metadata storage")
      metadataStorage.get.write(entityname, insertion.select(pk, metadataFieldNames.filterNot(x => x == pk).toSeq : _*), SaveMode.Append)
    }

    if (CatalogOperator.listIndexes(entityname).nonEmpty) {
      log.warn("new data inserted, but indexes are not updated; please re-create index")
      CatalogOperator.updateIndexesToStale(entityname)
    }

    //reset count
    tupleCount = -1

    Success(null)
  }

  /**
    * Returns a map of properties to the entity. Useful for printing.
    */
  def properties(implicit ac: AdamContext) : Map[String, String] = {
    val lb = ListBuffer[(String, String)]()


    lb.append("hasMetadata" -> hasMetadata.toString)
    lb.append("schema" -> schema.map(field => field.name + "(" + field.dataType.simpleString + ")").mkString(","))
    lb.append("indexes" -> CatalogOperator.listIndexes(entityname).mkString(", "))

    lb.toMap
  }

  /**
    *
    * @param filter
    * @return
    */
  def filter(filter: DataFrame)(implicit ac: AdamContext): DataFrame = {
    featureStorage.read(entityname).get.join(filter, pk)
  }

  /**
    *
    * @return
    */
  def schema(implicit ac: AdamContext) = if (metaData.isDefined) {
    featureData.join(metaData.get, pk).schema
  } else {
    featureData.schema
  }

  /**
    *
    * @return
    */
  def rdd(implicit ac: AdamContext) = if (metaData.isDefined) {
    featureData.join(metaData.get).rdd
  } else {
    featureData.rdd
  }

  /**
    *
    * @return
    */
  def getFeaturedata(implicit ac: AdamContext): DataFrame = featureData

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
    //TODO: check dimensionality of field and compare to dimensionality of query
    true
  }
}

/**
  *
  * @param name
  * @param fieldtype
  * @param pk
  * @param unique
  * @param indexed
  */
case class FieldDefinition(name: String, fieldtype: FieldType, pk: Boolean = false, unique: Boolean = false, indexed: Boolean = false)


object Entity {
  type EntityName = EntityNameHolder
}