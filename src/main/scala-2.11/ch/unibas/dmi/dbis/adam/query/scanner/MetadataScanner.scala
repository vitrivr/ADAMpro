package ch.unibas.dmi.dbis.adam.query.scanner

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import org.apache.spark.sql.Row

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object MetadataScanner {
  def apply(entity: Entity, query : BooleanQuery) : Seq[Row] =
    entity.getMetadata.filter(query.getWhereClause()).collect()
}
