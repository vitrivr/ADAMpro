package ch.unibas.dmi.dbis.adam.query

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAFIndexer, VAVIndexer}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import ch.unibas.dmi.dbis.adam.test.AdamBaseTest

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class QueryTestSuite extends AdamBaseTest {
  feature("standard query") {
    /**
      *
      */
    scenario("perform a sequential query") {
      Given("an entity")
      val es = getGroundTruthEvaluationSet()

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.sequentialQuery(es.entity.entityname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")

      results.zip(es.nnResults).foreach {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a ecp index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, ECPIndexer(es.distance))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      println("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      Entity.drop(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a lsh index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, LSHIndexer(es.distance))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      println("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      Entity.drop(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a sh index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, SHIndexer(es.feature.toSeq().size))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      println("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      Entity.drop(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a vaf index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, VAFIndexer(es.distance))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }



    /**
      *
      */
    scenario("perform a vav index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, VAVIndexer(es.feature.toSeq().size, es.distance))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }



    /**
      *
      */
    scenario("perform a vaf index query and a boolean query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, VAFIndexer(es.distance))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val bq = BooleanQuery(es.where)
      val results = QueryHandler.indexQuery(index.indexname)(nnq, Option(bq), true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnbqResults).foreach {
        case(res,gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }

    //TODO: add tests on querying joined metadata
    //TODO: add test join metadata
  }

  feature("progressive query") {
    //TODO: add tests for prog query (e.g., are at the end the correct results returned)
    //TODO: add test for timed prog query (e.g. does it adhere to time limit)
  }
}
