package ch.unibas.dmi.dbis.adam.evaluation.grpc


import java.io.{BufferedWriter, File, FileWriter, PrintWriter}
import java.util.Calendar

import ch.unibas.dmi.dbis.adam.evaluation.AdamParEvalUtils
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) extends AdamParEvalUtils {
  val fw = new FileWriter("results2.txt", true)
  val bw = new BufferedWriter(fw)
  val out = new PrintWriter(bw)

  /**
    * Init-Values
    */
  val nTuples = 1e5.toInt
  val k = 100

  /**
    * Evaluation Code
    */
  val tupleSizes = Seq(1e5.toInt, 1e6.toInt, 1e7.toInt, 1e8.toInt)
  val dimensions = Seq(10, 50, 128, 500)
  val partitions = Seq(1, 2, 4, 8, 16, 32, 64, 128)
  val indices = Seq(IndexType.ecp, IndexType.vaf, IndexType.lsh, IndexType.pq)

  try
      for (tuples <- tupleSizes) {
        for (dim <- dimensions) {
          System.out.println("New Round! "+tuples+" "+dim)
          dropAllEntities()
          val eName = ("silvan" + Math.abs(Random.nextInt())).filter(_ != '0')

          definer.createEntity(CreateEntityMessage(eName, Seq(FieldDefinitionMessage.apply("id", FieldDefinitionMessage.FieldType.LONG, true, true, true), FieldDefinitionMessage("feature", FieldDefinitionMessage.FieldType.FEATURE, false, false, true))))

          definer.generateRandomData(GenerateRandomDataMessage(eName, tuples, dim))

          val countedTuples = definer.count(EntityNameMessage(eName))
          if(countedTuples.message.toInt!= tuples){
            System.err.println(tuples + " | "+countedTuples)
          }

          for (index <- indices) {
            val name = generateIndex(index, eName)
            for (part <- partitions) {
              definer.repartitionIndexData(RepartitionMessage(name, part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING))
              val (time, noResults) = timeQuery(name, dim, part)
              appendToResults(tuples, dim, part, index.name, time, k, noResults)
            }
          }
        }
      }
  finally out.close


  def timeQuery(indexName: String, dim: Int, part: Int): (Float, Int) = {
    //1 free query to cache Index
    val res = searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage(dim, part)), from = Some(FromMessage(FromMessage.Source.Index(indexName)))))
    //System.out.println(indexName + " - " + res.responses.head.results.size)
    if (k > res.responses.head.results.size) {
      System.err.println("Should be " + k + ", but actually only " + res.responses.head.results.size)
    }

    var resSize = 0

    //Average over 10 Queries
    val start = System.currentTimeMillis()
    var counter = 0
    while (counter < 10) {
      val res = searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage(dim, part)), from = Some(FromMessage(FromMessage.Source.Index(indexName)))))
      resSize+=res.responses.head.results.size
      counter += 1
    }
    val stop = System.currentTimeMillis()
    ((stop - start)/10f,resSize/10)
  }

  def randomQueryMessage(dim: Int, part: Int) = NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dim)(Random.nextFloat())))), None, getDistanceMsg, k, Map[String, String](), true, 1 until part)

  def dropAllEntities() = {
    val entityList = definer.listEntities(EmptyMessage())

    for (entity <- entityList.entities) {
      //System.out.println("Dropping " + entity)
      val dropEnt = definer.dropEntity(EntityNameMessage(entity))
    }
  }

  def getDistanceMsg: Option[DistanceMessage] = Some(DistanceMessage(DistanceMessage.DistanceType.minkowski, Map[String, String](("norm", "2"))))

  def generateIndex(indexType: IndexType, eName: String): String = {
    val indexMsg = IndexMessage(eName, "feature", indexType, getDistanceMsg, Map[String, String]())
    val indexRes = definer.index(indexMsg)
    indexRes.message
  }

  def appendToResults(tuples: Int, dimensions: Int, partitions: Int, index: String, time: Float, k: Int, noResults: Int): Unit = {
    out.println(Calendar.getInstance().getTime()+","+index + "," + tuples + "," + dimensions + "," + partitions + "," + time + "," + k+", "+ noResults)
    out.flush()
  }


}

object RPCClient {
  def apply(host: String, port: Int): RPCClient = {
    val channel = OkHttpChannelBuilder.forAddress(host, port).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    new RPCClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}