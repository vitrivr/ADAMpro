package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.RandomDataOp.VectorDataGenerationDetails
import ch.unibas.dmi.dbis.adam.api.{RandomDataOp, EntityOp}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class SolrHandlerTestSuite extends AdamTestBase {

  scenario("create an entity") {
    withEntityName { entityname =>
      val ntuples = 1000

      val handlerName = Some("storing-solr")
      val fieldTypes = Seq(FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.TEXTTYPE, FieldTypes.BOOLEANTYPE)
      val attributes = fieldTypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("pkfield", FieldTypes.AUTOTYPE, true, storagehandlername = handlerName))

      EntityOp.create(entityname, attributes)

      RandomDataOp.apply(entityname, ntuples, Some(VectorDataGenerationDetails(10, 0, 0, 1, false)))

      val data = Entity.load(entityname).get.getData().get.collect()

      data.foreach {
        datum => //this should give an error if not possible
          val intfield = datum.getAs[Int]("integerfield")
          val longfield = datum.getAs[Long]("longfield")
          val floatfield = datum.getAs[Float]("floatfield")
          val doublefield = datum.getAs[Double]("doublefield")
          val stringfield = datum.getAs[String]("stringfield")
          val textfield = datum.getAs[String]("textfield")
          val booleanfield = datum.getAs[Boolean]("booleanfield")
      }

      assert(data.size == ntuples)
    }
  }

}
