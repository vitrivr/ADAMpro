package ch.unibas.dmi.dbis.adam.evaluation.grpc


import ch.unibas.dmi.dbis.adam.evaluation.{AdamParEvalUtils, EvaluationResultLogger}
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.collection.mutable
import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) extends AdamParEvalUtils with EvaluationResultLogger {

  val k = 100

  /**
    * Evaluation Params
    */
  val indexOnly = true
  val numQ = 2
  val tupleSizes = Seq(1e5.toInt)
  val dimensions = Seq(10)
  val partitions = Seq(4, 16, 200)
  val indices = Seq(IndexType.sh)
  val partitioners = Seq( RepartitionMessage.Partitioner.CURRENT, RepartitionMessage.Partitioner.SPARK)

  dropAllEntities()

  try
      for (tuples <- tupleSizes) {
        for (dim <- dimensions) {
          System.out.println(" \n ------------------ \n New Round! " + tuples + " " + dim)
          val eName = getOrGenEntity(tuples, dim)
          //Index generation
          for (index <- indices) {
            val name = getOrGenIndex(index, eName)
          }
          for (index <- indices) {
            var name = getOrGenIndex(index, eName)
            for (part <- partitions) {
              System.out.println("Repartitioning: " + part)
              for (partitioner <- partitioners) {
                System.out.println("\n ---------------------- \n Repartitioning with " + partitioner.name + ", partitions: "+part +" index: "+index)

                val repmsg = definer.repartitionIndexData(RepartitionMessage(name, numberOfPartitions = part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner))
                System.out.println("Repartition Message: "+repmsg.message)
                name = repmsg.message
                val props = definer.getEntityProperties(EntityNameMessage(eName))

                System.out.println("\n ----------------------- \n Repartitioned, entity Properties: \n "+props +" \n ------------------------- \n")
                System.out.println("----------------")

                val (avgTime, noResults) = timeQuery(name, dim, part)
                //TODO Maybe Result Quality
                appendToResults(tuples, dim, part, index.name, avgTime, k, noResults, partitioner)
              }
            }
          }
        }
      }
  finally out.close

  /** Checks if an Entity with the given Tuple size and dimensions exists */
  def getOrGenEntity(tuples: Int, dim: Int): String = {
    var eName: Option[String] = None
    val entitymessage = definer.listEntities(EmptyMessage())

    for (entity <- entitymessage.entities) {
      val c = definer.count(EntityNameMessage(entity))
      if (c.message.toInt == tuples) {
        val props = definer.getEntityProperties(EntityNameMessage(entity))
        //TODO Verify Dimension Count
        System.out.println(props)
        eName = Some(entity)
        System.out.println("Entity found - " + eName.get)
      }
    }

    if (eName.isEmpty) {
      System.out.println("Generating new Entity")
      eName = Some(("silvan" + Math.abs(Random.nextInt())).filter(_ != '0'))
      definer.createEntity(CreateEntityMessage(eName.get, Seq(AttributeDefinitionMessage.apply("id", AttributeType.LONG, true, true, true), AttributeDefinitionMessage("feature", AttributeType.FEATURE, false, false, true))))
      val options = Map("fv-dimensions" -> dim, "fv-min" -> 0, "fv-max" -> 1, "fv-sparse" -> false).mapValues(_.toString)
      definer.generateRandomData(GenerateRandomDataMessage(eName.get, tuples, options))
    }

    eName.get
  }

  /** Checks if Index exists and generates it otherwise */
  def getOrGenIndex(index: IndexType, eName: String): String = {
    val indexList = definer.listIndexes(EntityNameMessage(eName))
    var name = ""
    if (!indexList.indexes.exists(el => el.indextype == index)) {
      System.out.println("Index " + index.name + " does not exist, generating... ")
      name = generateIndex(index, eName)
    } else name = indexList.indexes.find(im => im.indextype == index).get.index
    System.out.println("Index name: " + name)

    name
  }

  //TODO Log individual queries in chronos
  def timeQuery(indexName: String, dim: Int, part: Int): (Float, Int) = {
    val queryCount = numQ
    val res = searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage(dim, part)), from = Some(FromMessage(FromMessage.Source.Index(indexName)))))

    //Average over Queries
    var resSize = 0
    val start = System.currentTimeMillis()
    var counter = 0
    while (counter < queryCount) {
      val qm = QueryMessage(nnq = Some(randomQueryMessage(dim, part)), from = Some(FromMessage(FromMessage.Source.Index(indexName))), information = Seq(QueryMessage.InformationLevel.WITH_PROVENANCE_PARTITION_INFORMATION, QueryMessage.InformationLevel.WITH_PROVENANCE_SOURCE_INFORMATION))
      val res = searcherBlocking.doQuery(qm)

      val partInfo = mutable.HashMap[Int, Int]()
      res.responses.map(f => f.results.map(r => {
        val key = r.data.getOrElse("ap_partition", DataMessage.defaultInstance).getIntData
        val value = partInfo.getOrElse(key, 0)
        partInfo.put(key, value + 1)
      }))
      System.out.println("Query done ,Partition Info: " + partInfo.toString())
      System.out.println("Reponses size: "+res.responses.head.results.size)
      System.out.println("Sample response: "+res.responses.head.results.head.data.mkString(", "))

      //TODO Does take take from partitions?
      val sorted = res.responses.head.results.sortBy(f => f.data.get("ap_distance").get.getFloatData)

      val top100Info = mutable.HashMap[Int, Int]()
      sorted.take(100).map(f => {
        val key = f.data.getOrElse("ap_partition", DataMessage.defaultInstance).getIntData
        val value = top100Info.getOrElse(key, 0)
        top100Info.put(key, value + 1)
      })
      System.out.println("Top 100 Partition Info: "+top100Info.mkString(", "))
      System.out.println("\n ----------------- \n")
      resSize += res.responses.head.results.size
      counter += 1
    }
    val stop = System.currentTimeMillis()
    ((stop - start) / queryCount.toFloat, resSize / queryCount)
  }

  /** Generates a random query using Random.nextFloat() */
  def randomQueryMessage(dim: Int, part: Int) = NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dim)(Random.nextFloat())))), None, getDistanceMsg, k, Map[String, String](), indexOnly = indexOnly, 1 until part)

  /** Drops all entities */
  def dropAllEntities() = {
    val entityList = definer.listEntities(EmptyMessage())

    for (entity <- entityList.entities) {
      System.out.println("Dropping Entity: " + entity)
      try {
        val props = definer.getEntityProperties(EntityNameMessage(entity))
        System.out.println(props)
      }
      catch {
        case e: Exception => System.err.println("Error in reading properties")
      }
      val dropEnt = definer.dropEntity(EntityNameMessage(entity))
      if (dropEnt.code.isError) {
        System.err.println("Error when dropping Entity " + entity + ": " + dropEnt.message)
      }
      System.out.println("\n Dropped Entity \n")
    }
  }

  /** Generates DistanceMessage with Minkowski-norm 2 */
  def getDistanceMsg: Option[DistanceMessage] = Some(DistanceMessage(DistanceMessage.DistanceType.minkowski, Map[String, String](("norm", "2"))))

  /** generates Index and returns the name */
  def generateIndex(indexType: IndexType, eName: String): String = {
    val indexMsg = IndexMessage(eName, "feature", indexType, getDistanceMsg, Map[String, String]())
    val indexRes = definer.index(indexMsg)
    indexRes.message
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