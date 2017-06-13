package org.vitrivr.adampro.storage

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.{EntityOp, QueryOp, RandomDataOp}
import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.entity.{AttributeDefinition, Entity}
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.query.handler.external.GenericExternalScanExpression
import org.vitrivr.adampro.query.query.BooleanQuery

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class PostGisTestSuite extends AdamTestBase {
  val handlerName = "postgis"
  def ntuples() = Random.nextInt(500)
  val fieldTypes = Seq(AttributeTypes.GEOMETRYTYPE)

  assert(ac.storageHandlerRegistry.contains(handlerName))

  scenario("create an entity") {
    val tuplesInsert = ntuples()

    withEntityName { entityname =>
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, tuplesInsert, Map())

      val data = Entity.load(entityname).get.getData().get.collect()

      data.foreach {
        datum => //this should give an error if not possible
          val geometryfield = datum.getAs[String]("geometryfield")
      }

      val count = data.size
      assert(count == tuplesInsert)
    }
  }

  scenario("put query to an entity") {
    val tuplesInsert = ntuples()

    withEntityName { entityname =>
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, tuplesInsert, Map())

      val query = "(ST_Distance('LINESTRING(-122.33 47.606, 0.0 51.5)'::geography, 'POINT(-21.96 64.15)':: geography))::int"
      val ncollect = 10
      val tracker = new OperationTracker()

      val results = QueryOp.compoundQuery(GenericExternalScanExpression(entityname, "postgis", Map("query" -> query, "limit" -> ncollect.toString))(ac))(tracker).get.get

      val collected = results.collect()
      val count = collected.length
      assert(count == ncollect)

      assert(collected.map(_.getInt(0)).forall(_ == 122235))
    }
  }
}
