package ch.unibas.dmi.dbis.adam.query

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.api.CompoundQueryOp.IntersectExpression
import ch.unibas.dmi.dbis.adam.api.QueryOp.SpecifiedIndexQueryHolder
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAFIndexer, VAVIndexer}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler
import ch.unibas.dmi.dbis.adam.query.progressive.ProgressiveQueryStatus
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import ch.unibas.dmi.dbis.adam.test.AdamTestBase
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration.Duration

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class QueryTestSuite extends AdamTestBase with ScalaFutures {
  val log = Logger.getLogger(getClass.getName)

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
        case (res, gt) =>
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
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.get.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

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
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.get.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

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
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.get.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

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
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.get.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a vaf index query without specifying index name") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = Index.createIndex(es.entity, VAFIndexer(es.distance))

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(es.entity.entityname, IndexTypes.VAFINDEX)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
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
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryHandler.indexQuery(index.get.indexname)(nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
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
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val bq = BooleanQuery(es.where)
      val results = QueryHandler.indexQuery(index.get.indexname)(nnq, Option(bq), true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnbqResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a boolean query on the joined metadata") {
      Given("an entity and a joinable table")
      val es = getGroundTruthEvaluationSet()

      val df = es.fullData

      val metadataname = es.entity.entityname + "_metadata"

      val metadata = df.withColumn("tid100", df("tid") * 100).select("tid", "tid100")

      SparkStartup.metadataStorage.write(metadataname, metadata)

      When("performing a boolean query on the joined metadata")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)

      val inStmt = "IN " + es.nnbqResults.map {
        case (distance, tid) =>
          (tid * 100).toString
      }.mkString("(", ", ", ")")
      val whereStmt = Seq("tid100" -> inStmt)

      val bq = BooleanQuery(whereStmt, Some(Seq((metadataname, Seq("tid")))))
      val results = QueryHandler.sequentialQuery(es.entity.entityname)(nnq, Option(bq), true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnbqResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
      SparkStartup.metadataStorage.drop(metadataname)
    }

  }

  feature("progressive query") {
    /**
      *
      */
    scenario("perform a progressive query") {
      Given("an entity")
      val es = getGroundTruthEvaluationSet()

      Index.createIndex(es.entity, SHIndexer(es.feature.toSeq().size))
      Index.createIndex(es.entity, VAFIndexer(es.distance))

      def processResults(status: ProgressiveQueryStatus.Value, df: DataFrame, confidence: Float, deliverer : String, info: Map[String, String]) {
        val results = df.map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        Then("eventually we should retrieve the k nearest neighbors")
        if (confidence - 1.0 < EPSILON) {
          results.zip(es.nnResults).foreach {
            case (res, gt) =>
              assert(res._2 == gt._2)
              assert(res._1 - gt._1 < EPSILON)
          }
        }
      }

      When("performing a kNN progressive query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val statusTracker = QueryHandler.progressiveQuery(es.entity.entityname)(nnq, None, processResults, true)

      whenReady(statusTracker) { result =>
        Then("the confidence should be 1.0")
        assert(result.confidence - 1.0 < EPSILON)
        assert(result.results.count() >= es.k)
      }

        //clean up
      Entity.drop(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a timed query") {
      Given("an entity")
      val es = getGroundTruthEvaluationSet()

      val timelimit = Duration(10, TimeUnit.SECONDS)

      When("performing a kNN progressive query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val (tpqRes, confidence, deliverer) = QueryHandler.timedProgressiveQuery(es.entity.entityname)(nnq, None, timelimit, true)

      Then("we should have a match at least in the first element")
      val results = tpqRes.map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
        .sortBy(_._1).toSeq

      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(res._1 - gt._1 < EPSILON)
      }

      //clean up
      Entity.drop(es.entity.entityname)
    }
  }



  feature("compound query") {
    /**
      *
      */
    scenario("perform a compound query") {
      Given("an entity and some indices")
      val es = getGroundTruthEvaluationSet()

      val shidx = IndexOp(es.entity.entityname, IndexTypes.SHINDEX, EuclideanDistance)
      val vaidx = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)

      When("performing a kNN query of two indices and performing the intersect")
      //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k, false, Map("numOfQ" -> "0"))

      val shqh = SpecifiedIndexQueryHolder(shidx.get.indexname, nnq, None, true)
      val vhqh = SpecifiedIndexQueryHolder(vaidx.get.indexname, nnq, None, true)

      val results = CompoundQueryOp.apply(new IntersectExpression(shqh, vhqh))
        .map(r => (r.getAs[Long](FieldNames.idColumnName))).collect().sorted

      //results
      val shres = shqh.eval().map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      val vhres = vhqh.eval().map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      
      Then("we should have a match in the aggregated list")
      val gt = vhres.intersect(shres).sorted

      results.zip(gt).map {
        case (res, gt) =>
          assert(res == gt)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }

  }
}
