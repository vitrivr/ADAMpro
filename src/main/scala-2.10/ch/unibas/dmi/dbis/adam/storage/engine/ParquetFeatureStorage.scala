package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import org.apache.spark.sql.{DataFrame, SaveMode}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object ParquetFeatureStorage extends FeatureStorage {
  /**
    * Read entity from feature storage.
    *
    * @param entityname
    * @param filter
    * @return
    */
  override def read(entityname: EntityName, filter: Option[Set[TupleID]])(implicit ac: AdamContext): DataFrame = {
    val df = ac.sqlContext.read.parquet(AdamConfig.indexPath + "/data/" + entityname + ".parquet")

    if(filter.isDefined){
      df.filter(df(FieldNames.idColumnName) isin (filter.get.toSeq : _*))
    } else {
      df
    }
  }

  /**
    * Count the number of tuples in the feature storage.
    *
    * @param entityname
    * @return
    */
  override def count(entityname: EntityName)(implicit ac: AdamContext): Int = {
    read(entityname).count().toInt
  }

  /**
    * Drop the entity from the feature storage
    *
    * @param entityname
    * @return true on success
    */
  override def drop(entityname: EntityName)(implicit ac: AdamContext): Boolean = ???

  /**
    * Write entity to the feature storage.
    *
    * @param entityname
    * @param df
    * @param mode
    * @return true on success
    */
  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode)(implicit ac: AdamContext): Boolean = {
    df.write.mode(mode).parquet(AdamConfig.indexPath + "/data/" + entityname + ".parquet")
    true
  }
}
