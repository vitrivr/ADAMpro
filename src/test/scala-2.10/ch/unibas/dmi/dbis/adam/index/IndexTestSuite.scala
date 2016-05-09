package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{DropEntityOp, DropIndexOp, IndexOp}
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.{FieldTypes, FieldDefinition, EntityHandler}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StructField, StructType}

import scala.util.Random

import SparkStartup.Implicits._


/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class IndexTestSuite extends AdamTestBase {
  SparkStartup

  val ntuples = Random.nextInt(1000)
  val ndims = 100

  feature("index creation") {
    /**
      *
      */
    scenario("create and drop indexes") {
      Given("an entity without metadata and an index")
      withSimpleEntity(ntuples, ndims) { entityname =>
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("creating the index")
        val index = IndexOp(entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index should be created")
        assert(IndexHandler.exists(index.get.indexname))

        When("dropping the index")
        DropIndexOp(index.get.indexname)

        Then("the index should be dropped")
        assert(!IndexHandler.exists(index.get.indexname))
      }
    }

    /**
      *
      */
    scenario("create and drop indexes of entity with multiple features") {
      Given("an entity with multiple feature vector fields without metadata")
      withEntityName { entityname =>
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("feature1", FieldTypes.FEATURETYPE), FieldDefinition("feature2", FieldTypes.FEATURETYPE)))

        val schema = StructType(Seq(
          StructField("idfield", LongType, false),
          StructField("feature1", new FeatureVectorWrapperUDT, false),
          StructField("feature2", new FeatureVectorWrapperUDT, false)
        ))
        val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(Random.nextLong(), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
        ))
        val data = ac.sqlContext.createDataFrame(rdd, schema)
        EntityHandler.insertData(entityname, data)


        When("creating the first index")
        val index1 = IndexOp(entityname, "feature1", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index1.isSuccess)

        Then("the index should be created")
        assert(IndexHandler.exists(index1.get.indexname))

        When("creating the second index")
        val index2 = IndexOp(entityname, "feature1", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index2.isSuccess)

        Then("the index should be created")
        assert(IndexHandler.exists(index2.get.indexname))

        When("dropping the first index")
        DropIndexOp(index1.get.indexname)

        Then("the first index should be dropped")
        assert(!IndexHandler.exists(index1.get.indexname))
        assert(IndexHandler.exists(index2.get.indexname))

        When("dropping the second index")
        DropIndexOp(index2.get.indexname)

        Then("the second index should be dropped")
        assert(!IndexHandler.exists(index1.get.indexname))
        assert(!IndexHandler.exists(index2.get.indexname))
      }
    }

    /**
      *
      */
    scenario("create and drop indexes by dropping entity") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata and an index")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("creating the index")
        val index = IndexOp(entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index should be created")
        assert(IndexHandler.exists(index.get.indexname))

        When("dropping the entity")
        DropEntityOp(entityname)

        Then("the index should be dropped")
        assert(!IndexHandler.exists(index.get.indexname))
      }
    }

    /**
      *
      */
    scenario("create eCP index") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("an eCP index is created")
        val index = IndexOp(entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index has been created")
        assert(IndexHandler.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence <= 1)
        And("all elements are indexed")
        assert(index.get.count == entity.get.count)
      }
    }

    /**
      *
      */
    scenario("create LSH index") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("an LSH index is created")
        val index = IndexOp(entityname, "feature", IndexTypes.LSHINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index has been created")
        assert(IndexHandler.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence <= 1)
        And("all elements are indexed")
        assert(index.get.count == entity.get.count)
      }
    }

    /**
      *
      */
    scenario("create PQ index") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("an SH index is created")
        val index = IndexOp(entityname, "feature", IndexTypes.PQINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index has been created")
        assert(IndexHandler.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence <= 1)
        And("all elements are indexed")
        assert(index.get.count == entity.get.count)
      }
    }

    /**
      *
      */
    scenario("create SH index") {
      withSimpleEntity(ntuples, ndims) { entityname =>

        Given("an entity without metadata")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("an SH index is created")
        val index = IndexOp(entityname, "feature", IndexTypes.SHINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index has been created")
        assert(IndexHandler.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence <= 1)
        And("all elements are indexed")
        assert(index.get.count == entity.get.count)
      }
    }


    /**
      *
      */
    scenario("create VA-File (fixed) index") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("an VA-File index is created")
        val index = IndexOp(entityname, "feature", IndexTypes.VAFINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index has been created")
        assert(IndexHandler.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence == 1)
        And("all elements are indexed")
        assert(index.get.count == entity.get.count)

        //clean up
        DropEntityOp(entityname)
      }
    }


    /**
      *
      */
    scenario("create VA-File (variable) index") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("an VA-File index is created")
        val index = IndexOp(entityname, "feature", IndexTypes.VAVINDEX, EuclideanDistance)
        assert(index.isSuccess)

        Then("the index has been created")
        assert(IndexHandler.exists(index.get.indexname))
        And("and the confidence is set properly")
        assert(index.get.confidence == 1)
        And("all elements are indexed")
        assert(index.get.count == entity.get.count)
      }
    }
  }
}
