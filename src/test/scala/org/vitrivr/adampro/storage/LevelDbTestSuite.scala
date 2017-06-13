package org.vitrivr.adampro.storage

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.{EntityOp, RandomDataOp}
import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.entity.{AttributeDefinition, Entity}

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class LevelDbTestSuite  extends AdamTestBase {
  val handlerName = "leveldb"
  def ntuples() = Random.nextInt(500)
  val attributetypes = Seq(AttributeTypes.VECTORTYPE)

  assert(ac.storageHandlerRegistry.contains(handlerName))

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