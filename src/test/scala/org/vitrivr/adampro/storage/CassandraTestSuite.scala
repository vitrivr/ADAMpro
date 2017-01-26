package org.vitrivr.adampro.storage

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.{EntityOp, RandomDataOp}
import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.entity.{AttributeDefinition, Entity}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class CassandraTestSuite  extends AdamTestBase {
  val ntuples = 1000

  val handlerName = "cassandra"
  val attributetypes = Seq(AttributeTypes.VECTORTYPE)

  scenario("create an entity") {
    withEntityName { entityname =>
      val attributes = attributetypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, ntuples, Map("fv-dimensions" -> 10.toString))

      val data = Entity.load(entityname).get.getData().get.collect()

      assert(data.size == ntuples)
    }
  }
}