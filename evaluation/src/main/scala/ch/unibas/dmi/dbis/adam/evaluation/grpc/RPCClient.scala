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
  val indices = Seq(IndexType.sh, IndexType.vaf)
  val partitioners = Seq( RepartitionMessage.Partitioner.CURRENT, RepartitionMessage.Partitioner.SPARK)

  var dropPartitions = Seq(0.0, 0.5)

  var eName = ""

  dropAllEntities()

  try
      for (tuples <- tupleSizes) {
        for (dim <- dimensions) {
          System.out.println(" \n ------------------ \n New Round! " + tuples + " " + dim)
          eName = getOrGenEntity(tuples, dim)
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
                name = repmsg.message

                val props = definer.getEntityProperties(EntityNameMessage(eName))
                System.out.println(props.properties.get("indexes").get)

                //TODO Index Partition Distribution
                for(dropPerc <- dropPartitions){
                  val (avgTime, noResults, informationloss) = timeQuery(name, dim, part, dropPerc)

                  appendToResults(tuples, dim, part, index.name, avgTime, k, noResults, partitioner, informationloss, dropPerc)
                }
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
      definer.createEntity(CreateEntityMessage(eName.get, Seq(AttributeDefinitionMessage.apply("pk", AttributeType.LONG, true, true, true), AttributeDefinitionMessage("feature", AttributeType.FEATURE, false, false, true))))
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
  def timeQuery(indexName: String, dim: Int, part: Int, dropPerc : Double): (Float, Int, Double) = {
    //Free query to cache index
    val nnq = Some(randomQueryMessage(dim, part, 0.0))
    searcherBlocking.doQuery(QueryMessage(nnq = nnq, from = Some(FromMessage(FromMessage.Source.Index(indexName)))))

    val queryCount = numQ

    //Average over Queries
    var resSize = 0
    var time = 0l
    var queryCounter = 0
    var avgMiss = 0.0

    while (queryCounter < queryCount) {
      System.out.println("--------- new Query, dropping: "+dropPerc+" of Partitions --------")

      //Skipping Query
      val start = System.currentTimeMillis()
      val nnq = Some(randomQueryMessage(dim, part, dropPerc))
      val qm = QueryMessage(nnq = nnq, from = Some(FromMessage(FromMessage.Source.Index(indexName))), information = Seq(QueryMessage.InformationLevel.WITH_PROVENANCE_PARTITION_INFORMATION, QueryMessage.InformationLevel.WITH_PROVENANCE_SOURCE_INFORMATION, QueryMessage.InformationLevel.INFORMATION_FULL_TREE))
      val dropRes = searcherBlocking.doQuery(qm)
      val stop = System.currentTimeMillis()
      time+=(stop-start)

      //truth
      val opt = collection.mutable.Map() ++ nnq.get.options
      opt -= "skipPart"
      val truthM = QueryMessage(nnq = Some(nnq.get.copy(options = opt.toMap, indexOnly = false)), from = Some(FromMessage(FromMessage.Source.Entity(eName))), information = qm.information, hints = Seq("sequential"))
      val gtruth = searcherBlocking.doQuery(truthM)

      //no skipping
      val noSkipOpt = collection.mutable.Map() ++ nnq.get.options
      noSkipOpt -= "skipPart"
      val noSkipRes = searcherBlocking.doQuery(QueryMessage(nnq = Some(qm.nnq.get.copy(options = noSkipOpt.toMap)), from = qm.from, information = qm.information))

      //System.out.println(dropRes.responses.head.results.size+" | "+dropRes.responses.head.results.head.data.mkString(", "))
      //System.out.println(gtruth.responses.head.results.size+" | "+gtruth.responses.head.results.head.data.mkString(", "))
      //System.out.println(noSkipRes.responses.head.results.size+" | "+noSkipRes.responses.head.results.head.data.mkString(", "))

      //Comparison Code
      val ratio = errorRatio(noSkipRes, dropRes)
      System.out.println("Error Ratio between skip and no skip: "+ratio)

      val agreements = topKMatch(gtruth, noSkipRes)
      System.out.println("Top K Matches between truth and no skip: "+agreements)

      val skipAgree = topKMatch(gtruth, dropRes)
      System.out.println("Top K Matches between truth and skip "+skipAgree)

      resSize += dropRes.responses.head.results.size
      queryCounter += 1
      avgMiss+=(agreements-skipAgree)
    }


    (time/ queryCount.toFloat, resSize / queryCount, avgMiss.toFloat/queryCount.toFloat)
  }

  def errorRatio(truth: QueryResultsMessage, guess:QueryResultsMessage) : Double = {
    var perfectMatches = 0
    val truths = truth.responses.head.results.sortBy(_.data.get("ap_distance").get.getFloatData).zipWithIndex
    val guesses = guess.responses.head.results.sortBy(_.data.get("ap_distance").get.getFloatData).zipWithIndex.toArray
    val errors: Seq[Float] = truths.map(el => {
      if(el._2>=k) 0f else {
        if(el._1.data.get("ap_distance").get.getFloatData == 0){
          perfectMatches+=1
          0f
        }else{
          if(el._2>=guesses.size){
            0f
          }else{
            (guesses(el._2)._1.data.get("ap_distance").get.getFloatData)/(el._1.data.get("ap_distance").get.getFloatData)
          }
        }
      }
    })
    if(perfectMatches>20){
      System.out.println(perfectMatches+" perfect Matches. There might be a problem in the code")
    }
    var err = 0.0
    for(f <- errors){
      if(err!=Double.NaN){
        err+=f
      }
    }
    err/Math.min(k, guesses.size)
  }

  def topKMatch(truth: QueryResultsMessage, guess: QueryResultsMessage) : Double = {
    val gtruthPKs = truth.responses.head.results.map(_.data.get("pk"))
    val resPKs = guess.responses.head.results.map(_.data.get("pk"))

    val ag= gtruthPKs.intersect(resPKs).length

    //simple hits/total
    ag
  }

  def printPartInfo(res: QueryResultsMessage): Unit = {
    val partInfo = mutable.HashMap[Int, Int]()
    res.responses.map(f => f.results.map(r => {
      val key = r.data.getOrElse("ap_partition", DataMessage.defaultInstance).getIntData
      val value = partInfo.getOrElse(key, 0)
      partInfo.put(key, value + 1)
      System.out.println("Query done ,Partition Info: " + partInfo.toString())
      System.out.println("Sample response: " + res.responses.head.results.head.data.mkString(", "))

      val sorted = res.responses.head.results.sortBy(f => f.data.get("ap_distance").get.getFloatData)

      val top100Info = mutable.HashMap[Int, Int]()
      sorted.take(100).map(f => {
        val key = f.data.getOrElse("ap_partition", DataMessage.defaultInstance).getIntData
        val value = top100Info.getOrElse(key, 0)
        top100Info.put(key, value + 1)
      })
      System.out.println("Top 100 Partition Info: " + top100Info.mkString(", "))
      System.out.println("\n ----------------- \n")
    }))
  }

  /** Generates a random query using Random.nextFloat() */
  def randomQueryMessage(dim: Int, part: Int, skip: Double) = NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dim)(Random.nextFloat())))), None, getDistanceMsg, k, Map[String, String]("skipPart" -> skip.toString), indexOnly = indexOnly)

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