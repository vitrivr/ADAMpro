package org.vitrivr.adampro.query

import java.util.concurrent.TimeUnit

import org.scalatest.concurrent.ScalaFutures
import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api._
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.TupleID.TupleID
import org.vitrivr.adampro.datatypes.vector.Vector
import org.vitrivr.adampro.helpers.tracker.OperationTracker
import org.vitrivr.adampro.index.Index._
import org.vitrivr.adampro.index.structures.IndexTypes
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.vitrivr.adampro.query.distance.{Distance, EuclideanDistance}
import org.vitrivr.adampro.query.handler.internal.AggregationExpression.{ExpressionEvaluationOrder, FuzzyIntersectExpression, IntersectExpression}
import org.vitrivr.adampro.query.handler.internal.{CompoundQueryExpression, IndexScanExpression, StochasticIndexQueryExpression}
import org.vitrivr.adampro.query.progressive.{AllProgressivePathChooser, ProgressiveObservation}
import org.vitrivr.adampro.query.query.{BooleanQuery, NearestNeighbourQuery, Predicate}

import scala.concurrent.duration.Duration
import scala.util.Try


/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class QueryTestSuite extends AdamTestBase with ScalaFutures {

  import ac.spark.implicits._

  feature("standard query") {
    /**
      *
      */
    scenario("perform a sequential query") {
      withQueryEvaluationSet { es =>
        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
        val tracker = new OperationTracker()
        val results = QueryOp.sequential(es.entity.entityname, nnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
        tracker.cleanAll()
      }
    }

    scenario("perform a weighted sequential query") {
      withQueryEvaluationSet { es =>
        When("performing a kNN query")
        val weights = Vector.conv_draw2vec(Seq.fill(es.vector.length)(Vector.zeroValue))
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, Some(weights), es.distance, es.k, false, es.options)
        val tracker = new OperationTracker()
        val results = QueryOp.sequential(es.entity.entityname, nnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.foreach { res =>
          assert(res._1 == Distance.zeroValue)
        }
        tracker.cleanAll()
      }
    }

    def indexQuery(es: EvaluationSet, indextypename: IndexTypeName, matchAll: Boolean): Unit = {
      Given("an index")
      val index = IndexOp.create(es.entity.entityname, "vectorfield", indextypename, es.distance)()
      assert(index.isSuccess)

      When("performing a kNN query")
      val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
      val tracker = new OperationTracker()
      val results = QueryOp.index(index.get.indexname, nnq, None)(tracker).get.get
        .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      if (matchAll) {
        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }
      } else {
        Then("we should have a match at least in the first element")

        log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

        assert(results.head._2 == es.nnResults.head._2)
        assert(math.abs(results.head._1 - es.nnResults.head._1) < EPSILON)
      }

      tracker.cleanAll()
    }

    /**
      *
      */
    scenario("perform a ecp index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.ECPINDEX, false) }
    }

    /**
      *
      */
    scenario("perform a lsh index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.LSHINDEX, false) }
    }

    /**
      *
      */
    scenario("perform a mi index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.MIINDEX, false) }

    }

    /**
      *
      */
    scenario("perform a pq index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.PQINDEX, false) }

    }


    /**
      *
      */
    scenario("perform a sh index query") {
      withQueryEvaluationSet { es =>
        val matchAll = false
        val indextypename = IndexTypes.SHINDEX

        Given("an index")
        val index = IndexOp.create(es.entity.entityname, "vectorfield", indextypename, EuclideanDistance)()
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, EuclideanDistance, es.k, false, es.options)
        val tracker = new OperationTracker()
        val results = QueryOp.index(index.get.indexname, nnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        if (matchAll) {
          Then("we should retrieve the k nearest neighbors")
          results.zip(es.nnResults).foreach {
            case (res, gt) =>
              assert(res._2 == gt._2)
              assert(math.abs(res._1 - gt._1) < EPSILON)
          }
        } else {
          Then("we should have a match at least in the first element")

          log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

          assert(results.head._2 == es.nnResults.head._2)
          assert(math.abs(results.head._1 - es.nnResults.head._1) < EPSILON)
        }

        tracker.cleanAll()
      }
    }


    /**
      *
      */
    scenario("perform a vaf index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.VAFINDEX, true) }
    }

    /**
      *
      */
    scenario("perform a vaf index query without specifying index name") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
        val tracker = new OperationTracker()
        val results = QueryOp.entityIndex(es.entity.entityname, IndexTypes.VAFINDEX, nnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        tracker.cleanAll()
      }
    }


    /**
      *
      */
    scenario("perform a vav index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.VAVINDEX, false) }
    }

    /**
      *
      */
    scenario("perform a vap index query") {
      withQueryEvaluationSet { es => indexQuery(es, IndexTypes.VAPLUSINDEX, false) }
    }

    /**
      *
      */
    scenario("perform a vaf index query and a boolean query") {
      withQueryEvaluationSet { es =>
        Given("an index")
        val index = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()
        assert(index.isSuccess)

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
        val bq = BooleanQuery(es.where)
        val tracker = new OperationTracker()
        val results = QueryOp.index(index.get.indexname, nnq, Option(bq))(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors")
        results.zip(es.nnbqResults).foreach {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        tracker.cleanAll()
      }
    }

    /**
      *
      */
    scenario("perform a simple Boolean query (without NN)") {
      withQueryEvaluationSet { es =>
        Given("a joinable table")

        val df = es.fullData

        When("performing a boolean query on the joined metadata")
        val tids = es.nnbqResults.map {
          case (distance, tid) =>
            (tid).toLong
        }
        val whereStmt = Seq(new Predicate("tid", Some("="), tids))

        val bq = BooleanQuery(whereStmt)
        val tracker = new OperationTracker()

        val results = QueryOp.booleanQuery(es.entity.entityname, Option(bq))(tracker).get.get
          .map(r => r.getAs[Long]("tid")).collect() //get here TID of metadata
          .sorted.toSeq

        Then("we should retrieve the metadata")
        results.zip(es.nnbqResults.map(_._2).sorted).foreach {
          case (res, gt) =>
            assert(res == gt)
        }

        tracker.cleanAll()
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
        IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.SHINDEX, es.distance)()
        IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()

        def processResults(tpo: Try[ProgressiveObservation]) {
          if (tpo.isFailure) {
            assert(false)
          }

          val po = tpo.get

          val results = po.results.get.map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
            .sortBy(_._1).toSeq

          Then("eventually we should retrieve the k nearest neighbors")
          if (math.abs(po.confidence - 1.0) < EPSILON) {

            results.zip(es.nnResults).foreach {
              case (res, gt) =>
                assert(res._2 == gt._2)
                assert(math.abs(res._1 - gt._1) < EPSILON)
            }
          }
        }

        When("performing a kNN progressive query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
        val tracker = new OperationTracker()
        val pqtracker = QueryOp.progressive(es.entity.entityname, nnq, None, new AllProgressivePathChooser(), processResults)(tracker).get

        whenReady(pqtracker) { result =>
          Then("the confidence should be 1.0")
          assert(math.abs(result.observation.confidence - 1.0) < EPSILON)
          assert(result.observation.results.get.count() >= es.k)

          tracker.cleanAll()
        }
      }
    }


    /**
      *
      */
    scenario("perform a timed query") {
      withQueryEvaluationSet { es =>
        Given("some indexes")
        IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.LSHINDEX, es.distance)()
        IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()

        val timelimit = Duration(10, TimeUnit.SECONDS)

        When("performing a kNN progressive query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
        val tracker = new OperationTracker()
        val po = QueryOp.timedProgressive(es.entity.entityname, nnq, None, new AllProgressivePathChooser(), timelimit)(tracker).get

        Then("we should have a match at least in the first element")
        val results = po.results.get.map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        tracker.cleanAll()
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

        val pqidx = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.PQINDEX, es.distance)()
        val vaidx = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()

        When("performing a kNN query of two indices and performing the intersect")
        //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, true, es.options ++ Map("numOfQ" -> "0"), None)
        val tracker = new OperationTracker()

        val pqqh = new IndexScanExpression(pqidx.get.indexname)(nnq, None)(None)(ac)
        val vhqh = new IndexScanExpression(vaidx.get.indexname)(nnq, None)(None)(ac)

        val results = time {
          new CompoundQueryExpression(new IntersectExpression(pqqh, vhqh)).prepareTree().evaluate()(tracker).get
            .map(r => (r.getAs[TupleID](AttributeNames.internalIdColumnName))).collect().sorted
        }

        val pqres = pqqh.evaluate()(tracker).get.map(r => r.getAs[TupleID](AttributeNames.internalIdColumnName)).collect()
        val vhres = vhqh.evaluate()(tracker).get.map(r => r.getAs[TupleID](AttributeNames.internalIdColumnName)).collect()

        Then("we should have a match in the aggregated list")
        val gt = vhres.intersect(pqres).sorted

        results.zip(gt).map {
          case (res, gt) =>
            assert(res == gt)
        }

        tracker.cleanAll()
      }
    }


    /**
      *
      */
    scenario("perform a compound query with the same index type at different coarseness levels") {
      withQueryEvaluationSet { es =>
        Given("some indexes")

        val vaidx1 = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance, Map("maxMarks" -> "4"))()
        val vaidx2 = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()

        When("performing a kNN query of two indices and performing the intersect")
        //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, true, es.options ++ Map("numOfQ" -> "0"), None)
        val tracker = new OperationTracker()

        val va1qh = new IndexScanExpression(vaidx1.get.indexname)(nnq, None)(None)(ac)
        val va2qh = new IndexScanExpression(vaidx2.get.indexname)(nnq, None)(None)(ac)

        val results = time {
          CompoundQueryExpression(new IntersectExpression(va1qh, va2qh, ExpressionEvaluationOrder.Parallel)).prepareTree().evaluate()(tracker).get
            .map(r => (r.getAs[TupleID](AttributeNames.internalIdColumnName))).collect().sorted
        }

        //results (note we truly compare the id-attribute here and not the metadata "tid"
        val vh1res = va1qh.evaluate()(tracker).get.map(r => r.getAs[TupleID](AttributeNames.internalIdColumnName)).collect()
        val vh2res = va2qh.evaluate()(tracker).get.map(r => r.getAs[TupleID](AttributeNames.internalIdColumnName)).collect()

        Then("we should have a match in the aggregated list")
        val gt = vh1res.intersect(vh2res).sorted

        results.zip(gt).map {
          case (res, gt) =>
            assert(res == gt)
        }

        tracker.cleanAll()
      }
    }
  }


  /**
    *
    */
  scenario("perform a compound query with fuzzy intersect") {
    withQueryEvaluationSet { es =>
      Given("some indexes")

      val vaidx1 = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance, Map("maxMarks" -> "4"))()
      val vaidx2 = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.VAFINDEX, es.distance)()

      When("performing a kNN query of two indices and performing the intersect")
      //nnq has numOfQ  = 0 to avoid that by the various randomized q's the results get different
      val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, true, es.options ++ Map("numOfQ" -> "0"), None)
      val tracker = new OperationTracker()

      val va1qh = new IndexScanExpression(vaidx1.get.indexname)(nnq, None)(None)(ac)
      val va2qh = new IndexScanExpression(vaidx2.get.indexname)(nnq, None)(None)(ac)

      val results = time {
        CompoundQueryExpression(new FuzzyIntersectExpression(va1qh, va2qh, ExpressionEvaluationOrder.Parallel))(ac).prepareTree().evaluate()(tracker).get
          .collect()
          .map(r => (r.getAs[TupleID](AttributeNames.internalIdColumnName), r.getAs[Distance.Distance](AttributeNames.distanceColumnName)))
          .sortBy(_._1)
      }

      //results (note we truly compare the id-attribute here and not the metadata "tid"
      val vh1res: Array[(TupleID, Distance)] = va1qh.evaluate()(tracker)(ac).get.collect().map(r => (r.getAs[TupleID](AttributeNames.internalIdColumnName), r.getAs[Distance.Distance](AttributeNames.distanceColumnName)))
      val vh2res: Array[(TupleID, Distance)] = va2qh.evaluate()(tracker)(ac).get.collect().map(r => (r.getAs[TupleID](AttributeNames.internalIdColumnName), r.getAs[Distance.Distance](AttributeNames.distanceColumnName)))

      Then("we should have a match in the aggregated list")
      val vh1map = vh1res.map(x => x._1 -> x._2).toMap
      val vh2map = vh2res.map(x => x._1 -> x._2).toMap

      val gt = (vh1map.keySet ++ vh2map.keySet).map(k =>
        (k, math.min(vh1map.getOrElse(k, 0.0), vh2map.getOrElse(k, 0.0)))
      ).toList.sortBy(_._1)

      results.zip(gt).map {
        case (res, gt) =>
          assert(res == gt)
      }

      tracker.cleanAll()
    }
  }


  feature("compound index scan") {
    scenario("perform a compound index scan query with various index types on the same entity") {
      withQueryEvaluationSet { es =>
        val index = IndexOp.generateAll(es.entity.entityname, "vectorfield", es.distance)()

        When("performing a kNN query")
        val nnq = NearestNeighbourQuery("vectorfield", es.vector, None, es.distance, es.k, false, es.options)
        val tracker = new OperationTracker()

        val indexscans = es.entity.indexes.map(index => IndexScanExpression(index.get)(nnq)()(ac))

        val results = StochasticIndexQueryExpression(indexscans)(nnq)()(ac).prepareTree()
          .evaluate()(tracker)(ac).get.map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(x => (x._1, x._2)).toSeq

        Then("we should retrieve the k nearest neighbors")
        Seq(results.zip(es.nnResults).head).map {
          case (res, gt) =>
            assert(res._2 == gt._2)
            assert(math.abs(res._1 - gt._1) < EPSILON)
        }

        log.info("score: " + getScore(es.nnResults.map(_._2), results.map(_._2)))

        tracker.cleanAll()
      }
    }
  }
}
