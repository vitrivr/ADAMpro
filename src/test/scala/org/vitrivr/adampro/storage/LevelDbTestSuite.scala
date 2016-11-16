package org.vitrivr.adampro.storage

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.{RandomDataOp, EntityOp}
import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.datatypes.feature.FeatureVectorWrapper
import org.vitrivr.adampro.entity.{Entity, AttributeDefinition}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class LevelDbTestSuite extends AdamTestBase {

  scenario("create an entity") {
    withEntityName { entityname =>
      val ntuples = 1000

      val handlerName = "leveldb"
      val fieldTypes = Seq(FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.BOOLEANTYPE, FieldTypes.FEATURETYPE)
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("id", FieldTypes.AUTOTYPE, true, storagehandlername = handlerName))

      EntityOp.create(entityname, attributes)

      RandomDataOp.apply(entityname, ntuples, Map("fv-dimensions" -> "10"))

      val data = Entity.load(entityname).get.getData().get.collect()

      data.foreach {
        datum => //this should give an error if not possible
          val intfield = datum.getAs[Int]("integerfield")
          val longfield = datum.getAs[Long]("longfield")
          val floatfield = datum.getAs[Float]("floatfield")
          val doublefield = datum.getAs[Double]("doublefield")
          val stringfield = datum.getAs[String]("stringfield")
          val booleanfield = datum.getAs[Boolean]("booleanfield")
          val featurefield = datum.getAs[FeatureVectorWrapper]("featurefield")
      }

      assert(data.size == ntuples)
    }
  }
}
