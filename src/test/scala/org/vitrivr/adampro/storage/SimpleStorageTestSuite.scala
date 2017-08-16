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
  * August 2017
  */
class SimpleStorageTestSuite extends AdamTestBase {
  def ntuples() = Random.nextInt(500)
  val attributetypes = Seq(AttributeTypes.VECTORTYPE)


  def storageTest(handlername : String) {
    assert(ac.storageManager.contains(handlername))

    val tuplesInsert = ntuples()

    withEntityName { entityname =>
      val attributes = attributetypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlername)) ++ Seq(AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = handlername))
      EntityOp.create(entityname, attributes)
      RandomDataOp.apply(entityname, tuplesInsert, Map("fv-dimensions" -> 10.toString))

      val data = Entity.load(entityname).get.getData().get.collect()

      assert(data.length == tuplesInsert)
    }
  }

  scenario("alluxio storage test") { storageTest("alluxio") }

  scenario("cassandra storage test") { storageTest("cassandra") }

  scenario("leveldb storage test") { storageTest("leveldb") }

  scenario("paldb storage test") { storageTest("paldb") }

  scenario("hbase storage test") { storageTest("hbase") }

  scenario("compound storage test") { storageTest("compound") }

}
