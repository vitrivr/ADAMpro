package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.exception.NoMetadataAvailableException
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
      if (query.join.isDefined) {
        val joins = query.join.get

        for (i <- (0 to joins.length)) {
          val join = joins(i)
          val newDF = SparkStartup.metadataStorage.read(join._1)
          df = df.join(newDF, join._2)
        }
      }

      Option(df.filter(query.getWhereClause()).select(FieldNames.idColumnName))
    } else {
      throw NoMetadataAvailableException()
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
    if (entity.hasMetadata) {
      val df = entity.getMetadata.get
      Option(df.filter(df(FieldNames.idColumnName) isin (filter.toSeq :_*)))
    } else {
      throw NoMetadataAvailableException()
    }
  }
}
