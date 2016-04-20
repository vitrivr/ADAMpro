package ch.unibas.dmi.dbis.adam.query

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.datastructures.CompoundQueryExpressions.{ExpressionEvaluationOrder, IntersectExpression}
import ch.unibas.dmi.dbis.adam.query.datastructures.{QueryCacheOptions, ProgressiveQueryStatus}
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.handler.CompoundQueryHandler
import ch.unibas.dmi.dbis.adam.query.handler.QueryHandler.SpecifiedIndexQueryHolder
import ch.unibas.dmi.dbis.adam.query.progressive.AllProgressivePathChooser
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration.Duration

import SparkStartup.Implicits._


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
      val results = QueryOp.sequential(es.entity.entityname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a ecp index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.ECPINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(index.get.indexname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      DropEntityOp(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a lsh index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.LSHINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(index.get.indexname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      DropEntityOp(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a pq index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.PQINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(index.get.indexname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a sh index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.SHINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(index.get.indexname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should have a match at least in the first element")
      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a vaf index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(index.get.indexname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }

    /**
      *
      */
    scenario("perform a vaf index query without specifying index name") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(es.entity.entityname, IndexTypes.VAFINDEX, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a vav index query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.VAVINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val results = QueryOp.index(index.get.indexname, nnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a vaf index query and a boolean query") {
      Given("an entity and an index")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val bq = BooleanQuery(es.where)
      val results = QueryOp.index(index.get.indexname, nnq, Option(bq), true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnbqResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
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
      val whereStmt = Option(Seq("tid100" -> inStmt))

      val bq = BooleanQuery(whereStmt, Some(Seq((metadataname, Seq("tid")))))
      val results = QueryOp.sequential(es.entity.entityname, nnq, Option(bq), true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors")
      results.zip(es.nnbqResults).foreach {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
      SparkStartup.metadataStorage.drop(metadataname)
    }

    /**
      * 
      */
    scenario("perform a simple Boolean query (without NN)") {
      Given("an entity and a joinable table")
      val es = getGroundTruthEvaluationSet()

      val df = es.fullData

      val metadataname = es.entity.entityname + "_metadata"

      val metadata = df.withColumn("tid100", df("tid") * 100).select("tid", "tid100")

      SparkStartup.metadataStorage.write(metadataname, metadata)

      When("performing a boolean query on the joined metadata")
      val inStmt = "IN " + es.nnbqResults.map {
        case (distance, tid) =>
          (tid * 100).toString
      }.mkString("(", ", ", ")")
      val whereStmt = Option(Seq("tid100" -> inStmt))

      val bq = BooleanQuery(whereStmt, Some(Seq((metadataname, Seq("tid")))))

      val results = QueryOp.booleanQuery(es.entity.entityname, Option(bq))
        .map(r => r.getAs[Long]("tid")).collect() //get here TID of metadata
        .sorted.toSeq

      Then("we should retrieve the metadata")
      results.zip(es.nnbqResults.map(_._2).sorted).foreach {
        case (res, gt) =>
          assert(res == gt)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
      SparkStartup.metadataStorage.drop(metadataname)
    }

  }

  feature("progressive query") {
    /**
      *
      */
    scenario("perform a progressive query") {
      Given("an entity and some indices")
      val es = getGroundTruthEvaluationSet()

      IndexOp(es.entity.entityname, IndexTypes.SHINDEX, EuclideanDistance)
      IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)

      def processResults(status: ProgressiveQueryStatus.Value, df: DataFrame, confidence: Float, source: String, info: Map[String, String]) {
        val results = df.map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("eventually we should retrieve the k nearest neighbors")
        if (math.abs(confidence - 1.0) < EPSILON) {

          results.zip(es.nnResults).foreach {
            case (res, gt) =>
              assert(res._2 == gt._2)
              assert(math.abs(res._1 - gt._1) < EPSILON)
          }
        }
      }

      When("performing a kNN progressive query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val statusTracker = QueryOp.progressive(es.entity.entityname, nnq, None, new AllProgressivePathChooser(), processResults, true)

      whenReady(statusTracker) { result =>
        Then("the confidence should be 1.0")
        assert(math.abs(result.confidence - 1.0) < EPSILON)
        assert(result.results.count() >= es.k)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a timed query") {
      Given("an entity and some indicies")
      val es = getGroundTruthEvaluationSet()

      IndexOp(es.entity.entityname, IndexTypes.SHINDEX, EuclideanDistance)
      IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)

      val timelimit = Duration(10, TimeUnit.SECONDS)

      When("performing a kNN progressive query")
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k)
      val (tpqRes, confidence, source) = QueryOp.timedProgressive(es.entity.entityname, nnq, None, new AllProgressivePathChooser(), timelimit, true)

      Then("we should have a match at least in the first element")
      val results = tpqRes.map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      Seq(results.zip(es.nnResults).head).map {
        case (res, gt) =>
          assert(res._2 == gt._2)
          assert(math.abs(res._1 - gt._1) < EPSILON)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }
  }



  feature("compound query") {
    /**
      *
      */
    scenario("perform a compound query with various index types") {
      Given("an entity and some indices")
      val es = getGroundTruthEvaluationSet()

      val shidx = IndexOp(es.entity.entityname, IndexTypes.SHINDEX, EuclideanDistance)
      val vaidx = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)

      When("performing a kNN query of two indices and performing the intersect")
      //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k, false, Map("numOfQ" -> "0"), None)

      val shqh = new SpecifiedIndexQueryHolder(shidx.get.indexname, nnq, None, None, Some(QueryCacheOptions()))
      val vhqh = new SpecifiedIndexQueryHolder(vaidx.get.indexname, nnq, None, None, Some(QueryCacheOptions()))

      val results = time {
        CompoundQueryHandler.indexOnlyQuery("")(new IntersectExpression(shqh, vhqh), false)
          .map(r => (r.getAs[Long](FieldNames.idColumnName))).collect().sorted
      }

      //results (note we truly compare the id-column here and not the metadata "tid"
      val shres = shqh.evaluate().map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      val vhres = vhqh.evaluate().map(r => r.getAs[Long](FieldNames.idColumnName)).collect()

      Then("we should have a match in the aggregated list")
      val gt = vhres.intersect(shres).sorted

      results.zip(gt).map {
        case (res, gt) =>
          assert(res == gt)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("perform a compound query with the same index type at different coarseness levels") {
      Given("an entity and some indices")
      val es = getGroundTruthEvaluationSet()

      val vaidx1 = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance, Map("maxMarks" -> "4"))
      val vaidx2 = IndexOp(es.entity.entityname, IndexTypes.VAFINDEX, EuclideanDistance)

      When("performing a kNN query of two indices and performing the intersect")
      //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
      val nnq = NearestNeighbourQuery(es.feature.vector, es.distance, es.k, false, Map("numOfQ" -> "0"), None)

      val va1qh = new SpecifiedIndexQueryHolder(vaidx1.get.indexname, nnq, None, None, Some(QueryCacheOptions()))
      val va2qh = new SpecifiedIndexQueryHolder(vaidx2.get.indexname, nnq, None, None, Some(QueryCacheOptions()))

      val results = time {
        CompoundQueryHandler.indexOnlyQuery("")(new IntersectExpression(va1qh, va2qh, ExpressionEvaluationOrder.LeftFirst), false)
          .map(r => (r.getAs[Long](FieldNames.idColumnName))).collect().sorted
      }

      //results (note we truly compare the id-column here and not the metadata "tid"
      val vh1res = va1qh.evaluate().map(r => r.getAs[Long](FieldNames.idColumnName)).collect()
      val vh2res = va2qh.evaluate().map(r => r.getAs[Long](FieldNames.idColumnName)).collect()

      Then("we should have a match in the aggregated list")
      val gt = vh1res.intersect(vh2res).sorted

      results.zip(gt).map {
        case (res, gt) =>
          assert(res == gt)
      }

      //clean up
      DropEntityOp(es.entity.entityname)
    }


  }
}
