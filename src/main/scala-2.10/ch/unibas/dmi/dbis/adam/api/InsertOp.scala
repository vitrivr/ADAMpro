package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import org.apache.spark.sql.DataFrame

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object InsertOp {
  /**
    *
    * @param entityname
    * @param df data frame containing all columns (of both the feature storage and the metadata storage);
    *                  note that you should name the feature column as ("feature").
    */
  def apply(entityname : EntityName, df : DataFrame): Unit = {
    Entity.insertData(entityname, df)
  }
}
