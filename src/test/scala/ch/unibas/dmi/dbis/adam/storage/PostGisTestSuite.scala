package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{QueryOp, RandomDataOp, EntityOp}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.{Entity, AttributeDefinition}
import ch.unibas.dmi.dbis.adam.query.handler.external.GisScanExpression
import ch.unibas.dmi.dbis.adam.query.query.BooleanQuery

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class PostGisTestSuite extends AdamTestBase {
  val ntuples = 1000

  val handlerName = Some("gis")
  val fieldTypes = Seq(FieldTypes.GEOMETRYTYPE)

  scenario("create an entity") {
    withEntityName { entityname =>
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("id", FieldTypes.AUTOTYPE, true, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, ntuples, Map())

      val data = Entity.load(entityname).get.getData().get.collect()

      data.foreach {
        datum => //this should give an error if not possible
          val geometryfield = datum.getAs[String]("geometryfield")
      }

      val count = data.size
      assert(count == ntuples)
    }
  }

  scenario("put query to an entity") {
    withEntityName { entityname =>
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("id", FieldTypes.AUTOTYPE, true, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, ntuples, Map())

      val query = "(ST_Distance('LINESTRING(-122.33 47.606, 0.0 51.5)'::geography, 'POINT(-21.96 64.15)':: geography))::int"
      val ncollect = 10

      val results = QueryOp.compoundQuery(GisScanExpression(entityname, "postgis", Map("query" -> query, "limit" -> ncollect.toString))).get.get

      val collected = results.collect()
      val count = collected.length
      assert(count == ncollect)

      assert(collected.map(_.getInt(0)).forall(_ == 122235))
    }
  }
}
