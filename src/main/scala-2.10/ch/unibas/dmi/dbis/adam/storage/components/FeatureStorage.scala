package ch.unibas.dmi.dbis.adam.storage.components

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SaveMode}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
trait FeatureStorage {
  /**
    * Create the entity in the feature storage.
    *
    * @param entityname
    * @return true on success
    */
  def create(entityname: EntityName): Boolean = {
    val featureSchema = StructType(
      List(
        StructField(FieldNames.idColumnName, LongType, false),
        StructField(FieldNames.internFeatureColumnName, ArrayType(FloatType), false)
      )
    )
    val df = SparkStartup.sqlContext.createDataFrame(SparkStartup.sc.emptyRDD[Row], featureSchema)

    write(entityname, df, SaveMode.Overwrite)
  }

  /**
    * Read entity from feature storage.
    *
    * @param entityname
    * @param filter
    * @return
    */
  def read(entityname: EntityName, filter: Option[scala.collection.Set[TupleID]] = None): DataFrame

  /**
    * Count the number of tuples in the feature storage.
    *
    * @param entityname
    * @return
    */
  def count(entityname: EntityName): Int

  /**
    * Write entity to the feature storage.
    *
    * @param entityname
    * @param data
    * @param mode
    * @return true on success
    */
  def write(entityname: EntityName, data: DataFrame, mode: SaveMode = SaveMode.Append): Boolean

  /**
    * Drop the entity from the feature storage
    *
    * @param entityname
    * @return true on success
    */
  def drop(entityname: EntityName): Boolean
}

