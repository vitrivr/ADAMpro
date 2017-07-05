package org.vitrivr.adampro.storage

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.communication.api.{EntityOp, RandomDataOp}
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.entity.{AttributeDefinition, Entity}

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class CassandraTestSuite  extends AdamTestBase {
  val handlerName = "cassandra"
  def ntuples() = Random.nextInt(500)
  val attributetypes = Seq(AttributeTypes.VECTORTYPE)

  assert(ac.storageManager.contains(handlerName))

  scenario("create an entity") {
    val tuplesInsert = ntuples()

    withEntityName { entityname =>
      val attributes = attributetypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, tuplesInsert, Map("fv-dimensions" -> 10.toString))

      val data = Entity.load(entityname).get.getData().get.collect()

      assert(data.size == tuplesInsert)
    }
  }
}