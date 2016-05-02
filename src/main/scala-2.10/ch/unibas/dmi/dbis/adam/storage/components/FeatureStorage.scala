package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapperUDT
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.util.Try

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait FeatureStorage {
  /**
    *
    * @param entityname
    * @return
    */
  def exists(entityname : EntityName) : Boolean

  /**
    * Create the entity in the feature storage.
    *
    * @param entityname
    * @return true on success
    */
  def create(entityname: EntityName)(implicit ac: AdamContext): Boolean = {
    val featureSchema = StructType(
      Seq(
        StructField(FieldNames.idColumnName, LongType, false),
        StructField(FieldNames.internFeatureColumnName,  new FeatureVectorWrapperUDT, false)
      )
    )
    val df = ac.sqlContext.createDataFrame(ac.sc.emptyRDD[Row], featureSchema)
    write(entityname, df, SaveMode.Overwrite)
  }

  /**
    * Read entity from feature storage.
    *
    * @param entityname
    * @param filter
    * @return
    */
  def read(entityname: EntityName, filter: Option[Set[TupleID]] = None)(implicit ac : AdamContext): Try[DataFrame]

  /**
    * Count the number of tuples in the feature storage.
    *
    * @param entityname
    * @return
    */
  def count(entityname: EntityName)(implicit ac: AdamContext): Long

  /**
    * Write entity to the feature storage.
    *
    * @param entityname
    * @param df
    * @param mode
    * @return true on success
    */
  def write(entityname: EntityName, df: DataFrame, mode: SaveMode = SaveMode.Append)(implicit ac: AdamContext): Boolean

  /**
    * Drop the entity from the feature storage
    *
    * @param entityname
    * @return true on success
    */
  def drop(entityname: EntityName)(implicit ac: AdamContext): Boolean
}

