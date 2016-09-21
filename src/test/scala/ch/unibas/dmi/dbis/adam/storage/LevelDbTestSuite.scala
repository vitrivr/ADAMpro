package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{RandomDataOp, EntityOp}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.{Entity, AttributeDefinition}

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

      val handlerName = Some("leveldb")
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
