package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{IndexOp, QueryOp}
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.{PartitionMode, Index, IndexLRUCache}
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.log4j.Logger

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
class PartitionTestSuite extends AdamTestBase {
  val log = Logger.getLogger(getClass.getName)

  val nPartitions = 8


  feature("repartitioning") {
    /**
      *
      */
    scenario("repartition replacing existing") {
      withQueryEvaluationSet { es =>
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a repartitioning with replacement")
        IndexOp.partition(index.get.indexname, nPartitions, None, Some(Seq("tid")), PartitionMode.REPLACE_EXISTING)

        val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

        val partresults = QueryOp.index(index.get.indexname, partnnq, None, false).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)
      }
    }


    /**
      *
      */
    scenario("repartition creating temporary new") {
      withQueryEvaluationSet { es =>
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a repartitioning with replacement")
        val partindex = IndexOp.partition(index.get.indexname, nPartitions, None, Some(Seq("tid")), PartitionMode.CREATE_TEMP)
        val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

        val partresults = QueryOp.index(partindex.get.indexname, partnnq, None, false).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        Then("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

        When("clearing the cache")
        IndexLRUCache.empty()

        Then("the index should be gone")
        val loadedIndex = Index.load(partindex.get.indexname)
        assert(loadedIndex.isFailure)
      }
    }

    /**
      *
      */
    scenario("repartition creating new") {
      withQueryEvaluationSet { es =>
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a repartitioning, creating new")
        val partindex = IndexOp.partition(index.get.indexname, nPartitions, None, Some(Seq("tid")), PartitionMode.CREATE_NEW)
        val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

        IndexLRUCache.empty()

        Then("we should be able to load the index")
        val loadedIndex = Index.load(partindex.get.indexname)
        assert(loadedIndex.isSuccess)

        val partresults = QueryOp.index(partindex.get.indexname, partnnq, None, false).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect()
          .sortBy(_._1).toSeq

        And("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)
      }
    }

    /**
      *
      */
    scenario("repartition based on metadata") {
      withQueryEvaluationSet { es =>
        val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
        assert(index.isSuccess)

        When("performing a repartitioning, creating new")
        val partindex = IndexOp.partition(index.get.indexname, nPartitions, None, Some(Seq("tid")), PartitionMode.CREATE_NEW)
        val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

        IndexLRUCache.empty()

        Then("we should be able to load the index")
        val loadedIndex = Index.load(partindex.get.indexname)
        assert(loadedIndex.isSuccess)

        val partresults = QueryOp.index(partindex.get.indexname, partnnq, None, true).get
          .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
          .sortBy(_._1).toSeq

        And("we should retrieve the k nearest neighbors of one partition only")
        assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)
      }
    }
  }
}
