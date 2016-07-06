package ch.unibas.dmi.dbis.adam.evaluation.grpc


import java.io.{BufferedWriter, FileWriter, PrintWriter}
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
  val fw = new FileWriter("results" + Calendar.getInstance().getTime.getTime + ".csv", true)
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
  val tupleSizes = Seq(1e5.toInt)
  val dimensions = Seq(128)
  val partitions = Seq(1, 2, 4, 8, 16, 32, 64, 128)
  val indices = Seq(IndexType.ecp, IndexType.vaf, IndexType.lsh, IndexType.pq, IndexType.sh)
  val partitioners = Seq(RepartitionMessage.Partitioner.CURRENT, RepartitionMessage.Partitioner.SPARK, RepartitionMessage.Partitioner.RANDOM)


  try
      for (tuples <- tupleSizes) {
        for (dim <- dimensions) {
          System.out.println("New Round! " + tuples + " " + dim)
          dropAllEntities()
          var eName = ""
          val entitymessage = definer.listEntities(EmptyMessage())
          //check if entity with given count exists
          for (entity <- entitymessage.entities) {
            val c = definer.count(EntityNameMessage(entity))
            if (c.message.toInt == tuples) {
              eName = entity
            }
          }

          if(eName.equals("")) {
            try {
              dropAllEntities()

            } catch {
              case e: Exception => System.err.println("Drop Entity failed")
            }
            eName = ("silvan" + Math.abs(Random.nextInt())).filter(_ != '0')

            definer.createEntity(CreateEntityMessage(eName, Seq(FieldDefinitionMessage.apply("id", FieldDefinitionMessage.FieldType.LONG, true, true, true), FieldDefinitionMessage("feature", FieldDefinitionMessage.FieldType.FEATURE, false, false, true))))

            definer.generateRandomData(GenerateRandomDataMessage(eName, tuples, dim))
          }

          //Index generation
          for (index <- indices) {
            //TODO Check if index exists
            val msg = definer.getEntityProperties(EntityNameMessage(eName))
            var name = ""
            if(!msg.properties.get("indexes").getOrElse("").contains(index.name)){
              name = generateIndex(index, eName)
              //TODO Ugly fix
            }else name = eName+"_feature_"+index.name+"_0"
            for (part <- partitions) {
              for (partitioner <- partitioners) {
                definer.repartitionIndexData(RepartitionMessage(name, part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner))
                val (avgTime, noResults) = timeQuery(name, dim, part)
                appendToResults(tuples, dim, part, index.name, avgTime, k, noResults, partitioner)
              }
            }
            try {
              //val res = definer.dropIndex(IndexNameMessage(name))
              //System.out.println(res)
            } catch {
              case e: Exception => System.err.println("Drop Index failed")
            }
          }
        }
      }
  finally out.close


  def timeQuery(indexName: String, dim: Int, part: Int): (Float, Int) = {

    val queryCount = 10
    //1 free query to cache Index
    val res = searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage(dim, part)), from = Some(FromMessage(FromMessage.Source.Index(indexName)))))
    if (k > res.responses.head.results.size) {
      System.err.println("Should be " + k + ", but actually only " + res.responses.head.results.size)
    }

    var resSize = 0

    //Average over Queries
    val start = System.currentTimeMillis()
    var counter = 0
    while (counter < queryCount) {
      val res = searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage(dim, part)), from = Some(FromMessage(FromMessage.Source.Index(indexName)))))
      resSize += res.responses.head.results.size
      counter += 1
    }
    val stop = System.currentTimeMillis()
    ((stop - start) / queryCount.toFloat, resSize / queryCount)
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

  def appendToResults(tuples: Int, dimensions: Int, partitions: Int, index: String, time: Float, k: Int = 0, noResults: Int = 0, partitioner: RepartitionMessage.Partitioner): Unit = {
    out.println(Calendar.getInstance().getTime() + "," + index + "," + tuples + "," + dimensions + "," + partitions + "," + time + "," + k + ", " + noResults + ", " + partitioner.name)
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