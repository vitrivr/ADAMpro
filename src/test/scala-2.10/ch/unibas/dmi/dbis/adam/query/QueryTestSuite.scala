package ch.unibas.dmi.dbis.adam.query

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import ch.unibas.dmi.dbis.adam.query.datastructures.CompoundQueryExpressions.{ExpressionEvaluationOrder, IntersectExpression}
import ch.unibas.dmi.dbis.adam.query.datastructures.{ProgressiveQueryStatus, QueryCacheOptions}
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.handler.internal.{CompoundQueryHolder, IndexQueryHolder}
import ch.unibas.dmi.dbis.adam.query.progressive.AllProgressivePathChooser
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}
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
  feature("standard query") {
    /**
      *
      */
    scenario("perform a sequential query") {
      withQueryEvaluationSet { es =>
        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.sequential(es.entity.entityname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      }
    }

    /**
      *
      */
    scenario("perform a ecp index query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(index.get.indexname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should have a match at least in the first element")
        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))
      }
    }

    /**
      *
      */
    scenario("perform a lsh index query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.LSHINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(index.get.indexname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should have a match at least in the first element")
        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))
      }
    }

    /**
      *
      */
    scenario("perform a pq index query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.PQINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(index.get.indexname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should have a match at least in the first element")
        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))
      }
    }


    /**
      *
      */
    scenario("perform a sh index query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.SHINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(index.get.indexname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should have a match at least in the first element")
        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))
      }
    }


    /**
      *
      */
    scenario("perform a vaf index query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(index.get.indexname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      }
    }

    /**
      *
      */
    scenario("perform a vaf index query without specifying index name") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(es.entity.entityname, IndexTypes.VAFINDEX, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      }
    }


    /**
      *
      */
    scenario("perform a vav index query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAVINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val results = QueryOp.index(index.get.indexname, nnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      }
    }


    /**
      *
      */
    scenario("perform a vaf index query and a boolean query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val bq = BooleanQuery(es.where)
        val results = QueryOp.index(index.get.indexname, nnq, Option(bq), true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnbqResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      }
    }


    /**
      *
      */
    scenario("perform a boolean query on the joined metadata") {
      withQueryEvaluationSet { es =>
        Given("a joinable table")

        val df = es.fullData

        val metadataname = es.entity.entityname + "_metadata"

        val metadata = df.withColumn("tid100", df("tid") * 100).select("tid", "tid100")

        SparkStartup.metadataStorage.write(metadataname, metadata)

        When("performing a boolean query on the joined metadata")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)

        val inStmt = "IN " + es.nnbqResults.map {
          case (distance, tid) =>
            (tid * 100).toString
        }.mkString("(", ", ", ")")
        val whereStmt = Option(Seq("tid100" -> inStmt))

        val bq = BooleanQuery(whereStmt, Some(Seq((metadataname, Seq("tid")))))
        val results = QueryOp.sequential(es.entity.entityname, nnq, Option(bq), true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnbqResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
        //clean up
        SparkStartup.metadataStorage.drop(metadataname)
      }
    }

    /**
      *
      */
    scenario("perform a simple Boolean query (without NN)") {
      withQueryEvaluationSet { es =>
        Given("a joinable table")

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

        val results = QueryOp.booleanQuery(es.entity.entityname, Option(bq)).get
          .map(r => r.getAs[Long]("tid")).collect() //get here TID of metadata
          .sorted.toSeq

        Then("we should retrieve the metadata")
        results.zip(es.nnbqResults.map(_._2).sorted).foreach {
          case (res, gt) =>
            assert(res == gt)
        }

        //clean up
        SparkStartup.metadataStorage.drop(metadataname)
      }
    }
  }

  feature("progressive query") {
    /**
      *
      */
    scenario("perform a progressive query") {
      withQueryEvaluationSet { es =>
        Given("some indexes")
        IndexOp(es.entity.entityname, "featurefield", IndexTypes.SHINDEX, EuclideanDistance)
        IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)

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
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val statusTracker = QueryOp.progressive(es.entity.entityname, nnq, None, new AllProgressivePathChooser(), processResults, true).get

        whenReady(statusTracker) { result =>
          Then("the confidence should be 1.0")
          assert(math.abs(result.confidence - 1.0) < EPSILON)
          assert(result.results.count() >= es.k)
        }
      }
    }


    /**
      *
      */
    scenario("perform a timed query") {
      withQueryEvaluationSet { es =>
        Given("some indexes")
        IndexOp(es.entity.entityname, "featurefield", IndexTypes.SHINDEX, EuclideanDistance)
        IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)

        val timelimit = Duration(10, TimeUnit.SECONDS)

        When("performing a kNN progressive query")
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k)
        val (tpqRes, confidence, source) = QueryOp.timedProgressive(es.entity.entityname, nnq, None, new AllProgressivePathChooser(), timelimit, true).get

        Then("we should have a match at least in the first element")
        val results = tpqRes.map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      }
    }
  }



  feature("compound query") {
    /**
      *
      */
    scenario("perform a compound query with various index types") {
      withQueryEvaluationSet { es =>
        Given("some indexes")

        val shidx = IndexOp(es.entity.entityname, "featurefield", IndexTypes.SHINDEX, EuclideanDistance)
        val vaidx = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)

        When("performing a kNN query of two indices and performing the intersect")
        //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map("numOfQ" -> "0"), None)

        val shqh = new IndexQueryHolder(shidx.get.indexname)(nnq, None, None, None, Some(QueryCacheOptions()))
        val vhqh = new IndexQueryHolder(vaidx.get.indexname)(nnq, None, None, None, Some(QueryCacheOptions()))

        val results = time {
          new CompoundQueryHolder(es.entity.entityname)(new IntersectExpression(shqh, vhqh)).evaluate()
            .map(r => (r.getAs[Long]("tid"))).collect().sorted
        }

        val shres = shqh.evaluate().map(r => r.getAs[Long]("tid")).collect()
        val vhres = vhqh.evaluate().map(r => r.getAs[Long]("tid")).collect()

        Then("we should have a match in the aggregated list")
        val gt = vhres.intersect(shres).sorted

        results.zip(gt).map {
          case (res, gt) =>
            assert(res == gt)
        }
      }
    }


    /**
      *
      */
    scenario("perform a compound query with the same index type at different coarseness levels") {
      withQueryEvaluationSet { es =>
        Given("some indexes")

        val vaidx1 = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance, Map("maxMarks" -> "4"))
        val vaidx2 = IndexOp(es.entity.entityname, "featurefield", IndexTypes.VAFINDEX, EuclideanDistance)

        When("performing a kNN query of two indices and performing the intersect")
        //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
        val nnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map("numOfQ" -> "0"), None)

        val va1qh = new IndexQueryHolder(vaidx1.get.indexname)(nnq, None, None, None, Some(QueryCacheOptions()))
        val va2qh = new IndexQueryHolder(vaidx2.get.indexname)(nnq, None, None, None, Some(QueryCacheOptions()))

        val results = time {
          CompoundQueryHolder(es.entity.entityname)(new IntersectExpression(va1qh, va2qh, ExpressionEvaluationOrder.Parallel)).evaluate()
            .map(r => (r.getAs[Long]("tid"))).collect().sorted
        }

        //results (note we truly compare the id-column here and not the metadata "tid"
        val vh1res = va1qh.evaluate().map(r => r.getAs[Long]("tid")).collect()
        val vh2res = va2qh.evaluate().map(r => r.getAs[Long]("tid")).collect()

        Then("we should have a match in the aggregated list")
        val gt = vh1res.intersect(vh2res).sorted

        results.zip(gt).map {
          case (res, gt) =>
            assert(res == gt)
        }
      }
    }
  }
}
