package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import org.apache.spark.sql.DataFrame

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
  def apply(entity: Entity, query: BooleanQuery): Option[DataFrame] = {
    if (entity.getMetadata.isDefined) {
      var df = entity.getMetadata.get

      //TODO: check join
      for (i <- (0 to query.join.length)) {
        val joinInfo = query.join(i)
        val newDF =  SparkStartup.metadataStorage.read(joinInfo._1)
        df = df.join(newDF, joinInfo._2)
      }
      
      Option(df.filter(query.getWhereClause()))
    } else {
      None
    }
  }

  /**
    * Performs a Boolean query on the metadata where the ID only is compared.
    *
    * @param entity
    * @param filter tuple ids to filter on
    * @return
    */
  def apply(entity: Entity, filter: HashSet[TupleID]): Option[DataFrame] = {
    if (entity.getMetadata.isDefined) {
      val df = entity.getMetadata.get
      Option(df.filter(df(FieldNames.idColumnName) isin filter))
    } else {
      None
    }
  }
}
