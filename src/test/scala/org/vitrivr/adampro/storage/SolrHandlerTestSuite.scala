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
  * July 2016
  */
class SolrHandlerTestSuite extends AdamTestBase {

  scenario("create an entity") {
    withEntityName { entityname =>
      val ntuples = Random.nextInt(500)

      val handlerName = "solr"
      val attributetypes = Seq(AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.FLOATTYPE, AttributeTypes.DOUBLETYPE, AttributeTypes.STRINGTYPE, AttributeTypes.TEXTTYPE, AttributeTypes.BOOLEANTYPE)
      val attributes = attributetypes.map(field => AttributeDefinition(field.name + "field", field, storagehandlername = handlerName)) ++ Seq(AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = handlerName))

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
          val textfield = datum.getAs[String]("textfield")
          val booleanfield = datum.getAs[Boolean]("booleanfield")
      }

      assert(data.size == ntuples)
    }
  }

}
