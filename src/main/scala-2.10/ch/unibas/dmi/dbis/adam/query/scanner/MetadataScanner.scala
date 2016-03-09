package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import org.apache.spark.sql.Row

import scala.collection.immutable.HashSet

/**
  * adamtwo
  *
  * Scans the metadata.
  *
  * Ivan Giangreco
  * November 2015
  */
object MetadataScanner {
  /**
    * Performs a Boolean query on the metadata.
    *
    * @param entity
    * @param query
    * @return
    */
  def apply(entity: Entity, query: BooleanQuery): Seq[Row] = {
    var df = entity.getMetadata
    df.filter(query.getWhereClause()).collect()
  }

  /**
    * Performs a Boolean query on the metadata where the ID only is compared.
    *
    * @param entity
    * @param filter tuple ids to filter on
    * @return
    */
  def apply(entity: Entity, filter: HashSet[TupleID]): Seq[Row] = {
    val df = entity.getMetadata
    df.filter(df(SparkStartup.featureStorage.idColumnName) isin filter).collect()
  }
}
