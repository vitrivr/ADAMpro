package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.api.{IndexOp, RandomDataOp}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.utils.AdamParUtils

import scala.util.Random

/**
  * Created by silvan on 06.06.16.
  *
  * Heavily copied from ADAMTestBase
  */
object SilvanPlayground extends AdamParUtils{

  val startup = SparkStartup

  def main(args: Array[String]) : Unit = {
    //Initial Parameters
    val name = "silvan"
    val dim = 128
    val collectionSize = 1e5.toInt

    //Entity Creation
    val pk = AttributeDefinition.apply("tid",FieldTypes.LONGTYPE,true,true,true)
    val featureVector = AttributeDefinition("feature",FieldTypes.FEATURETYPE,indexed = true)
    Entity.create(name,Seq(pk,featureVector))

    //Fill the Entity with data
    time("generating data")(RandomDataOp.apply(name,collectionSize,dim))

    //Setup Index. the val ecp should be the index that is created
    val ecp: Index = IndexOp.create(name,"feature",IndexTypes.ECPINDEX,EuclideanDistance).get

    //Generate initial NNQuery
    val initNNQuery = NearestNeighbourQuery("feature", new FeatureVectorWrapper(Seq.fill(dim)(Random.nextFloat())).vector ,weights =  None, distance = EuclideanDistance, k = 10)

    //Index.load
    val loadResults = time("\n load-scan") (Index.load(ecp.indexname, false).get.scan(initNNQuery,None))
    //This ensures every row is loaded
    time("Collecting scan-results")(loadResults.rdd.collect().toSeq.map(f => System.out.println(f.toString())))

    //Watch how this takes ages longer
    time("\n val-scan") (ecp.scan(initNNQuery,filter = None))

    //We do it again to test if it's not a cache-related issue
    val queryVector = new FeatureVectorWrapper(Seq.fill(dim)(Random.nextFloat())).vector

    val nnQuery = NearestNeighbourQuery("feature", queryVector ,weights =  None, distance = EuclideanDistance, k = 10)

    var results = time("\n ECP-loadscan 2") (Index.load(ecp.indexname, false).get.scan(nnQuery,None))
    //time("Collecting scan-results")(results.rdd.collect().toSeq.map(f => System.out.println(f.toString())))
    time("\n val-scan 2") (ecp.scan(nnQuery, None))
  }

}
