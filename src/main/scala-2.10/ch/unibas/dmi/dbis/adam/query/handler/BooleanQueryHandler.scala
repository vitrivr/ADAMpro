package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import ch.unibas.dmi.dbis.adam.query.scanner.MetadataScanner
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object BooleanQueryHandler extends Logging {
  /**
    * Performs a Boolean query on the metadata.
    *
    * @param entityname
    * @param query
    * @return
    */
  def metadataQuery(entityname: EntityName, query : BooleanQuery): Option[DataFrame] =
    MetadataScanner(Entity.load(entityname), query)

  /**
    * Performs a Boolean query on the metadata where the ID only is compared.
    *
    * @param entityname
    * @param filter tuple ids to filter on
    * @return
    */
  def metadataQuery(entityname: EntityName, filter: HashSet[TupleID]): Option[DataFrame] =
    MetadataScanner(Entity.load(entityname), filter)
}
