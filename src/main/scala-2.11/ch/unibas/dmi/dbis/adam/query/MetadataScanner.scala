package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import org.apache.spark.sql.Row


/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object MetadataScanner {
  def apply(entity : Entity, filter : Seq[TupleID]): collection.Map[Long, Row] = {
    entity.getMetadata.filter(entity.getMetadata("__adam_id") isin (filter : _*)).map(r => r.getAs[Long]("__adam_id") -> r).collectAsMap()
  }
}
