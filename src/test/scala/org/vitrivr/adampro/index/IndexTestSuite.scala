package org.vitrivr.adampro.index

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.{EntityOp, IndexOp}
import org.vitrivr.adampro.datatypes.AttributeTypes
import org.vitrivr.adampro.entity.{AttributeDefinition, Entity}
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.query.distance.EuclideanDistance
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.vitrivr.adampro.datatypes.vector.Vector

import scala.util.Random


/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class IndexTestSuite extends AdamTestBase {

  val ntuples = 1000 + Random.nextInt(1000)
  val ndims = 100


  /**
    *
    * @param indextype
    */
  def indexCreationTest(indextype : IndexTypes.IndexType) {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata")
        val entity = Entity.load(entityname)
        assert(entity.isSuccess)

        When("a " + indextype.toString() + " index is created")
        val index = IndexOp.create(entity.get.entityname, "feature", indextype, EuclideanDistance)()
        assert(index.isSuccess)

        Then("the index has been created")
        assert(Index.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence <= 1)
        And("all elements are indexed")
        assert(index.get.count == EntityOp.count(entityname).get)
      }
  }

  feature("index creation") {
    /**
      *
      */
    scenario("create and drop indexes") {
      Given("an entity without metadata and an index")
      withSimpleEntity(ntuples, ndims) { entityname =>
        val entity = Entity.load(entityname)
        assert(entity.isSuccess)

        When("creating the index")
        val index = IndexOp.create(entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)()

        if(index.isFailure){
          throw index.failed.get
        }


        assert(index.isSuccess)

        Then("the index should be created")
        assert(Index.exists(index.get.indexname))

        When("dropping the index")
        IndexOp.drop(index.get.indexname)

        Then("the index should be dropped")
        assert(!Index.exists(index.get.indexname))
      }
    }

    /**
      *
      */
    scenario("create and drop indexes of entity with multiple features") {
      Given("an entity with multiple feature vector fields without metadata")
      withEntityName { entityname =>
        Entity.create(entityname, Seq(new AttributeDefinition("idfield", AttributeTypes.LONGTYPE), new AttributeDefinition("feature1", AttributeTypes.VECTORTYPE), new AttributeDefinition("feature2", AttributeTypes.VECTORTYPE)))

        val schema = StructType(Seq(
          StructField("idfield", LongType, false),
          StructField("feature1", AttributeTypes.VECTORTYPE.datatype, false),
          StructField("feature2", AttributeTypes.VECTORTYPE.datatype, false)
        ))
        val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(Random.nextLong(), Seq.fill(ndims)(Vector.nextRandom()), Seq.fill(ndims)(Vector.nextRandom()))
        ))
        val data = ac.sqlContext.createDataFrame(rdd, schema)
        EntityOp.insert(entityname, data)


        When("creating the first index")
        val index1 = IndexOp.create(entityname, "feature1", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index1.isSuccess)

        Then("the index should be created")
        assert(Index.exists(index1.get.indexname))

        When("creating the second index")
        val index2 = IndexOp.create(entityname, "feature1", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index2.isSuccess)

        Then("the index should be created")
        assert(Index.exists(index2.get.indexname))

        When("dropping the first index")
        IndexOp.drop(index1.get.indexname)

        Then("the first index should be dropped")
        assert(!Index.exists(index1.get.indexname))
        assert(Index.exists(index2.get.indexname))

        When("dropping the second index")
        IndexOp.drop(index2.get.indexname)

        Then("the second index should be dropped")
        assert(!Index.exists(index1.get.indexname))
        assert(!Index.exists(index2.get.indexname))
      }
    }

    /**
      *
      */
    scenario("create and drop indexes by dropping entity") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata and an index")
        val entity = Entity.load(entityname)
        assert(entity.isSuccess)

        When("creating the index")
        val index = IndexOp.create(entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index.isSuccess)

        Then("the index should be created")
        assert(Index.exists(index.get.indexname))

        When("dropping the entity")
        EntityOp.drop(entityname)

        Then("the index should be dropped")
        assert(!Index.exists(index.get.indexname))
      }
    }

    /**
      *
      */
    scenario("create eCP index") {
      indexCreationTest(IndexTypes.ECPINDEX)
    }

    /**
      *
      */
    scenario("create LSH index") {
      indexCreationTest(IndexTypes.LSHINDEX)
    }

    /**
      *
      */
    scenario("create PQ index") {
      indexCreationTest(IndexTypes.PQINDEX)
    }

    /**
      *
      */
    scenario("create SH index") {
      indexCreationTest(IndexTypes.SHINDEX)
    }


    /**
      *
      */
    scenario("create VA-File (fixed) index") {
      indexCreationTest(IndexTypes.VAFINDEX)
    }


    /**
      *
      */
    scenario("create VA-File (variable) index") {
      indexCreationTest(IndexTypes.VAVINDEX)
    }

    /**
      *
      */
    scenario("create VA-File (plus) index") {
      indexCreationTest(IndexTypes.VAPLUSINDEX)
    }
  }
}
