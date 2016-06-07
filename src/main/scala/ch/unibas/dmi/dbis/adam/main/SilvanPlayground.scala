package ch.unibas.dmi.dbis.adam.main

import ch.unibas.dmi.dbis.adam.api.{EntityOp, IndexOp}
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits.sqlContext.implicits._
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.utils.AdamParUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random

/**
  * Created by silvan on 06.06.16.
  *
  * Heavily copied from ADAMTestBase
  */
object SilvanPlayground extends AdamParUtils{

  val startup = SparkStartup

  def main(args: Array[String]) : Unit = {
    val name = "silvan"
    val dim = 128
    //val noTuples = Seq(1e3, 1e4, 1e5, 1e6, 1e7).map(f => f.toInt)
    time("generate Data")(generateEntity(dim,1e5.toInt,name,10))
    val index = time("Setup Index") (generateIndex(name))

    var queryTime: List[Int] = List()

    val partitions = (1 until 10)

    val queryVector = createRandomVector(dim).vector

    for(i <- partitions){
      //Entity.load(name).get.data.get.repartition(i)

      //Index.repartition(index,5,None,None,PartitionMode.REPLACE_EXISTING)

      val nnQuery = NearestNeighbourQuery("feature", queryVector ,weights =  None, distance = EuclideanDistance, k = 10)

      val results: DataFrame = time("NNQuery "+i) (index.scan(nnQuery,None))
      val sortedResults = results.sort($"adamprodistance".asc)
      System.out.println(results.count())
      results.show()
      System.out.println(sortedResults.show())
      queryTime = queryTime.::(5)
    }
  }

  def generateEntity(dimensions: Int, tuples: Int, name: String, partitions: Int) : Unit = {
    val pk = AttributeDefinition.apply("tid",FieldTypes.LONGTYPE,true,true,true)
    val featureVector = AttributeDefinition("feature",FieldTypes.FEATURETYPE,indexed = true)
    Entity.create(name,Seq(pk,featureVector))


    def createTuple() : Row = {
      Row(Random.nextLong(), createRandomVector(dimensions))
    }

    //FeatureVectorWrapper is defined via the user-defined-type API From Spark
    val schema: StructType = StructType(Seq(StructField("tid", LongType), StructField("feature",new FeatureVectorWrapperUDT)))

    //Create data TODO Do we have a guarantee about the ordering of rows? No, right?
    //Resilient Distributed DataSet -> parallelize just returns an RDD of whatever the given codeblock returns
    val rdd: RDD[Row] = ac.sc.parallelize((0 until tuples).map(f => createTuple))

    //Now we  transform our RDD of rows to a DataFrame (Think distributed table)
    val data = ac.sqlContext.createDataFrame(rdd,schema)

    data.repartition(partitions)

    EntityOp.insert(name,data).get
  }

  def generateIndex(name : String)(implicit ac: AdamContext) : Index = {
    val index= IndexOp.create(name,"feature",IndexTypes.ECPINDEX,EuclideanDistance).get
    index
  }

  //FeatureVectorWrapper wraps a dense vector
  def createRandomVector(noDimensions : Int) : FeatureVectorWrapper = {
    new FeatureVectorWrapper(Seq.fill(noDimensions)(Random.nextFloat()))
  }

}
