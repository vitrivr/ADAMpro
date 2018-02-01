package org.vitrivr.adampro.data.entity

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.scalatest.Matchers._
import org.vitrivr.adampro.communication.api.{QueryOp, RandomDataOp}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.vector.{ADAMBit64Vector, ADAMBytesVector}
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.vitrivr.adampro.query.distance.HammingDistance
import org.vitrivr.adampro.query.query.RankingQuery
import org.vitrivr.adampro.query.tracker.QueryTracker

import scala.util.Random


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * February 2018
  */
class DataTestSuite extends AdamTestBase {

  def ntuples(min : Int = 1, max : Int = 1000) =  min + Random.nextInt(max - min)

  def ndims(min : Int = 20, max : Int = 100) = min + Random.nextInt(max - min)

  import ac.spark.implicits._

  feature("data types") {

    scenario("insert float vector") {
      withEntityName { entityname =>
        When("a new random entity (without any metadata) is created")
        val datatype = AttributeTypes.VECTORTYPE
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", datatype)))

        assert(entity.isSuccess)

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        When("data is inserted")
        val insert = RandomDataOp.apply(entityname, tuplesInsert, Map("fv-dimensions" -> dimsInsert.toString))
        assert(insert.isSuccess)


        Then("one entity should be created")
        val count = entity.get.count
        assert(count == tuplesInsert)

        val datum = entity.get.getFeatureData.get.head.getAs[Seq[Float]]("feature")
        assert(datum.length == dimsInsert)
      }
    }

    scenario("insert bit64 vector") {
      withEntityName { entityname =>
        When("a new random entity (without any metadata) is created")
        val datatype = AttributeTypes.BIT64VECTORTYPE
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", datatype)))

        assert(entity.isSuccess)

        val tuplesInsert = ntuples()

        When("data is inserted")
        val insert = RandomDataOp.apply(entityname, tuplesInsert, Map())
        assert(insert.isSuccess)


        Then("one entity should be created")
        val count = entity.get.count
        assert(count == tuplesInsert)

        val datum = entity.get.getFeatureData.get.head.getAs[Long]("feature")
      }
    }

    scenario("query bit64 vector") {
      withEntityName { entityname =>
        When("a new random entity (without any metadata) is created")
        val datatype = AttributeTypes.BIT64VECTORTYPE
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", datatype)))

        assert(entity.isSuccess)

        val tuplesInsert = ntuples(1000, 10000)
        val insert = RandomDataOp.apply(entityname, tuplesInsert, Map())

        val nnq = RankingQuery("feature", ADAMBit64Vector(Random.nextLong()), None, HammingDistance, 100, false, Map())
        val tracker = new QueryTracker()
        val results = QueryOp.sequential(entityname, nnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("idfield"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq
      }
    }

    scenario("insert bytes vector") {
      withEntityName { entityname =>
        When("a new random entity (without any metadata) is created")
        val datatype = AttributeTypes.BYTESVECTORTYPE
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", datatype)))

        assert(entity.isSuccess)

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        When("data is inserted")
        val insert = RandomDataOp.apply(entityname, tuplesInsert, Map("fv-dimensions" -> dimsInsert.toString))
        assert(insert.isSuccess)


        Then("one entity should be created")
        val count = entity.get.count
        assert(count == tuplesInsert)

        val datum = entity.get.getFeatureData.get.head.getAs[Array[Byte]]("feature")
        assert(datum.length * 8 > dimsInsert)
      }
    }


    scenario("query bytes vector") {
      withEntityName { entityname =>
        When("a new random entity (without any metadata) is created")
        val datatype = AttributeTypes.BYTESVECTORTYPE
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", datatype)))

        assert(entity.isSuccess)

        val tuplesInsert = ntuples(1000, 10000)
        val dimsInsert = ndims()
        val insert = RandomDataOp.apply(entityname, tuplesInsert, Map("fv-dimensions" -> dimsInsert.toString))

        val query = new ADAMBytesVector(RandomDataOp.generateArrayByte(dimsInsert))


        val nnq = RankingQuery("feature", query, None, HammingDistance, 100, false, Map())
        val tracker = new QueryTracker()
        val results = QueryOp.sequential(entityname, nnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("idfield"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq
      }
    }

  }
}
