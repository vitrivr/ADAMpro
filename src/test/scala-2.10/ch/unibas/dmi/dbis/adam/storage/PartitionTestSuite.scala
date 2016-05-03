package ch.unibas.dmi.dbis.adam.storage

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.{DropEntityOp, IndexOp, QueryOp}
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.{IndexHandler, IndexLRUCache}
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.partitions.{PartitionHandler, PartitionOptions}
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
      Given("an entity")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a repartitioning with replacement")
      PartitionHandler.repartitionIndex(index.get.indexname, nPartitions, false, Some(Seq(FieldNames.idColumnName)), PartitionOptions.REPLACE_EXISTING)

      val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

      val partresults = QueryOp.index(IndexHandler.load(index.get.indexname).get, partnnq, None, false)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long](FieldNames.idColumnName))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors of one partition only")
      assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

      //clean up
      DropEntityOp(es.entity.entityname)
    }


    /**
      *
      */
    scenario("repartition creating temporary new") {
      Given("an entity")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a repartitioning with replacement")
      val partindex = PartitionHandler.repartitionIndex(index.get.indexname, nPartitions, false, Some(Seq(FieldNames.idColumnName)), PartitionOptions.CREATE_TEMP)
      val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

      val partresults = QueryOp.index(IndexHandler.load(partindex.get.indexname).get, partnnq, None, false)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long](FieldNames.idColumnName))).collect()
        .sortBy(_._1).toSeq

      Then("we should retrieve the k nearest neighbors of one partition only")
      assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

      When("clearing the cache")
      IndexLRUCache.empty()

      Then("the index should be gone")
      val loadedIndex = IndexHandler.load(partindex.get.indexname)
      assert(loadedIndex.isFailure)

      //clean up
      DropEntityOp(es.entity.entityname)
    }

    /**
      *
      */
    scenario("repartition creating new") {
      Given("an entity")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a repartitioning with replacement")
      val partindex = PartitionHandler.repartitionIndex(index.get.indexname, nPartitions, false, Some(Seq(FieldNames.idColumnName)), PartitionOptions.CREATE_NEW)
      val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

      IndexLRUCache.empty()

      Then("we should be able to load the index")
      val loadedIndex = IndexHandler.load(partindex.get.indexname)
      assert(loadedIndex.isSuccess)

      val partresults = QueryOp.index(loadedIndex.get, partnnq, None, false)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long](FieldNames.idColumnName))).collect()
        .sortBy(_._1).toSeq

      And("we should retrieve the k nearest neighbors of one partition only")
      assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

      //clean up
      DropEntityOp(es.entity.entityname)
    }

    /**
      *
      */
    scenario("repartition based on metadata") {
      Given("an entity")
      val es = getGroundTruthEvaluationSet()
      val index = IndexOp(es.entity.entityname, "featurefield", IndexTypes.ECPINDEX, EuclideanDistance)
      assert(index.isSuccess)

      When("performing a repartitioning with replacement")
      val partindex = PartitionHandler.repartitionIndex(index.get.indexname, nPartitions, true, Some(Seq("tid")), PartitionOptions.CREATE_NEW)
      val partnnq = NearestNeighbourQuery("featurefield", es.feature.vector, es.distance, es.k, false, Map[String, String](), Some(Set(0)))

      IndexLRUCache.empty()

      Then("we should be able to load the index")
      val loadedIndex = IndexHandler.load(partindex.get.indexname)
      assert(loadedIndex.isSuccess)

      val partresults = QueryOp.index(loadedIndex.get, partnnq, None, true)
        .map(r => (r.getAs[Float](FieldNames.distanceColumnName), r.getAs[Long]("tid"))).collect() //get here TID of metadata
        .sortBy(_._1).toSeq

      And("we should retrieve the k nearest neighbors of one partition only")
      assert(partresults.map(x => x._2 % nPartitions).distinct.length == 1)

      //clean up
      DropEntityOp(es.entity.entityname)
    }
  }
}
