package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAFIndexer, VAVIndexer}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.test.AdamTestBase

import scala.util.Random

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
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("creating the index")
      val index = Index.createIndex(entity, ECPIndexer(EuclideanDistance))

      Then("the index should be created")
      assert(Index.exists(index.indexname))

      When("dropping the index")
      Index.drop(index.indexname)

      Then("the index should be dropped")
      assert(!Index.exists(index.indexname))

      //clean up
      Entity.drop(entityname)
    }

    /**
      *
      */
    scenario("create and drop indexes by dropping entity") {
      Given("an entity without metadata and an index")
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("creating the index")
      val index = Index.createIndex(entity, ECPIndexer(EuclideanDistance))

      Then("the index should be created")
      assert(Index.exists(index.indexname))

      When("dropping the entity")
      Entity.drop(entityname)

      Then("the index should be dropped")
      assert(!Index.exists(index.indexname))
    }

    /**
      *
      */
    scenario("create eCP index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("an eCP index is created")
      val index = Index.createIndex(entity, ECPIndexer(EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      And("and the confidence is set properly")
      assert(index.confidence <= 1)
      And("all elements are indexed")
      assert(index.count == entity.count)

      //clean up
      Entity.drop(entityname)
    }

    /**
      *
      */
    scenario("create LSH index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("an LSH index is created")
      val index = Index.createIndex(entity, LSHIndexer(EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      And("and the confidence is set properly")
      assert(index.confidence <= 1)
      And("all elements are indexed")
      assert(index.count == entity.count)

      //clean up
      Entity.drop(entityname)
    }

    /**
      *
      */
    scenario("create SH index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("an SH index is created")
      val index = Index.createIndex(entity, SHIndexer(ndims))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      And("and the confidence is set properly")
      assert(index.confidence <= 1)
      And("all elements are indexed")
      assert(index.count == entity.count)

      //clean up
      Entity.drop(entityname)
    }


    /**
      *
      */
    scenario("create VA-File (fixed) index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("an VA-File index is created")
      val index = Index.createIndex(entity, VAFIndexer(EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      And("and the confidence is set properly")
      assert(index.confidence == 1)
      And("all elements are indexed")
      assert(index.count == entity.count)

      //clean up
      Entity.drop(entityname)
    }


    /**
      *
      */
    scenario("create VA-File (variable) index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity(ntuples, ndims)
      val entity = Entity.load(entityname)

      When("an VA-File index is created")
      val index = Index.createIndex(entity, VAVIndexer(ndims, EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      And("and the confidence is set properly")
      assert(index.confidence == 1)
      And("all elements are indexed")
      assert(index.count == entity.count)

      //clean up
      Entity.drop(entityname)
    }
  }
}
