package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{EntityOp, RandomDataOp}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class CassandraTestSuite  extends AdamTestBase {
  val ntuples = 1000

  val handlerName = Some("kv")
  val fieldTypes = Seq(FieldTypes.FEATURETYPE)

  scenario("create an entity") {
    withEntityName { entityname =>
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("id", FieldTypes.AUTOTYPE, true, storagehandlername = handlerName))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, ntuples, Map("fv-dimensions" -> 10.toString))

      val data = Entity.load(entityname).get.getData().get.collect()

      assert(data.size == ntuples)
    }
  }
}