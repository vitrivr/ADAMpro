package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{DropEntityOp, DropIndexOp, IndexOp}
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance

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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)
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
    scenario("create and drop indexes by dropping entity") {
      withSimpleEntity(ntuples, ndims) { entityname =>
        Given("an entity without metadata and an index")
        val entity = EntityHandler.load(entityname)
        assert(entity.isSuccess)

        When("creating the index")
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)
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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.ECPINDEX, EuclideanDistance)
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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.LSHINDEX, EuclideanDistance)
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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.PQINDEX, EuclideanDistance)
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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.SHINDEX, EuclideanDistance)
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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.VAFINDEX, EuclideanDistance)
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
        val index = IndexOp(entity.get.entityname, "feature", IndexTypes.VAVINDEX, EuclideanDistance)
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
