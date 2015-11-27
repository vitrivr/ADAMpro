package ch.unibas.dmi.dbis.adam.query.handler

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery
import ch.unibas.dmi.dbis.adam.query.scanner.MetadataScanner
import org.apache.spark.Logging
import org.apache.spark.sql.Row

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object BooleanQueryHandler extends Logging {

  def metadataQuery(entityname: EntityName, query : BooleanQuery): Seq[Row] =
    MetadataScanner(Entity.retrieveEntity(entityname), query)


  def metadataQuery(entityname: EntityName, filter: HashSet[TupleID]): Seq[Row] =
    MetadataScanner(Entity.retrieveEntity(entityname), filter)
}
