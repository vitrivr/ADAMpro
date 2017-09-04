package org.vitrivr.adampro.storage

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.communication.api.{IndexOp, QueryOp}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.distribution.partitioning.PartitionMode
import org.vitrivr.adampro.query.distance.Distance.Distance
import org.vitrivr.adampro.query.distance.{Distance, EuclideanDistance}
import org.vitrivr.adampro.query.query.RankingQuery

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PartitionTestSuite extends AdamTestBase {

  import ac.spark.implicits._

  val nPartitions = 8


  feature("repartitioning index") {
    /**
      *
      */
    scenario("repartition index replacing existing") {
      withQueryEvaluationSet { es =>
        val index = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index.isSuccess)

        When("performing a repartitioning with replacement")
        IndexOp.partition(index.get.indexname, nPartitions, None, Some("tid"), PartitionMode.REPLACE_EXISTING)

        val partnnq = RankingQuery("vectorfield", es.vector, None, es.distance, es.k, false, Map[String, String](), Some(Set(0)))
        val tracker = new QueryTracker()

        val partresults = QueryOp.index(index.get.indexname, partnnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

        tracker.cleanAll()
      }
    }


    /**
      *
      */
    scenario("repartition index creating temporary new") {
      withQueryEvaluationSet { es =>
        val index = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index.isSuccess)

        When("performing a repartitioning with replacement")
        val partindex = IndexOp.partition(index.get.indexname, nPartitions, None, Some("tid"), PartitionMode.CREATE_TEMP)
        val partnnq = RankingQuery("vectorfield", es.vector, None, es.distance, es.k, false, Map[String, String](), Some(Set(0)))
        val tracker = new QueryTracker()

        val partresults = QueryOp.index(partindex.get.indexname, partnnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

        When("clearing the cache")
        ac.cacheManager.emptyIndex()

        Then("the index should be gone")
        val loadedIndex = Index.load(partindex.get.indexname)
        assert(loadedIndex.isFailure)

        tracker.cleanAll()
      }
    }

    /**
      *
      */
    scenario("repartition index creating new") {
      withQueryEvaluationSet { es =>
        val index = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index.isSuccess)

        When("performing a repartitioning, creating new")
        val partindex = IndexOp.partition(index.get.indexname, nPartitions, None, Some("tid"), PartitionMode.CREATE_NEW)
        val partnnq = RankingQuery("vectorfield", es.vector, None, es.distance, es.k, false, Map[String, String](), Some(Set(0)))
        val tracker = new QueryTracker()

        ac.cacheManager.emptyIndex()

        Then("we should be able to load the index")
        val loadedIndex = Index.load(partindex.get.indexname)
        assert(loadedIndex.isSuccess)

        val x = QueryOp.index(partindex.get.indexname, partnnq, None)(tracker).get.get

        val partresults = x
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        And("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

        tracker.cleanAll()
      }
    }

    /**
      *
      */
    scenario("repartition index based on metadata") {
      withQueryEvaluationSet { es =>
        val index = IndexOp.create(es.entity.entityname, "vectorfield", IndexTypes.ECPINDEX, EuclideanDistance)()
        assert(index.isSuccess)

        When("performing a repartitioning, creating new")
        val partindex = IndexOp.partition(index.get.indexname, nPartitions, None, Some("tid"), PartitionMode.CREATE_NEW)
        val partnnq = RankingQuery("vectorfield", es.vector, None, es.distance, es.k, false, Map[String, String](), Some(Set(0)))
        val tracker = new QueryTracker()

        ac.cacheManager.emptyIndex()

        Then("we should be able to load the index")
        val loadedIndex = Index.load(partindex.get.indexname)
        assert(loadedIndex.isSuccess)

        val partresults = QueryOp.index(partindex.get.indexname, partnnq, None)(tracker).get.get
          .map(r => (r.getAs[Distance](AttributeNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        And("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

        tracker.cleanAll()
      }
    }
  }
}
