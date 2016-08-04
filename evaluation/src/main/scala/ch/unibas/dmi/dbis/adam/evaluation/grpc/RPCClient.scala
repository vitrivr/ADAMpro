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
  super.setK(k)

  /**
    * Evaluation Params
    */
  val indexOnly = true
  val numQ = 20
  val tupleSizes = Seq(1e7.toInt, 1e8.toInt)
  val dimensions = Seq(10, 128)
  val partitions = Seq(10, 20, 50, 200, 1000)
  val indices = Seq(IndexType.sh, IndexType.vaf, IndexType.ecp, IndexType.lsh, IndexType.pq)
  val partitioners = Seq( RepartitionMessage.Partitioner.CURRENT, RepartitionMessage.Partitioner.ECP)

  var dropPartitions = Seq(0.0, 0.1, 0.2, 0.3, 0.4, 0.5)

  val queries = dimensions.map(f => f -> IndexedSeq.fill(numQ)(randomQueryMessage(f,0.0))).toMap

  val truths = scala.collection.mutable.Map[Int, Seq[QueryResultsMessage]]()
  val noSkipping = scala.collection.mutable.Map[Int, scala.collection.mutable.Map[IndexType, Seq[QueryResultsMessage]]]()

  var eName = ""

  dropAllEntities()

  try
      for (tuples <- tupleSizes) {
        super.setTuples(tuples)
        for (dim <- dimensions) {
          super.setDimensions(dim)
          System.out.println(" \n ------------------ \n New Round! " + tuples + " " + dim)
          eName = getOrGenEntity(tuples, dim)
          for (index <- indices) {
            val name = getOrGenIndex(index, eName)
          }
          System.out.println("Calculating Truths... ")
          truths += dim -> IndexedSeq.tabulate(numQ)( el => searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(getOrGenIndex(IndexType.vaf, eName)))), nnq = Some(queries.get(dim).get(el).copy(indexOnly = false)))))
          System.out.println("VA-Scans exectued")
          //System.out.println(truths.head._2.head.responses.head.results.head.data.mkString(", "))
          noSkipping.put(dim, mutable.Map[IndexType, Seq[QueryResultsMessage]]())

          for (index <- indices) {
            super.setIndex(index.name)
            var name = getOrGenIndex(index, eName)
            for (part <- partitions) {
              super.setPartitions(part)
              for (partitioner <- partitioners) {
                super.setPartitioner(partitioner)
                System.out.println("\n ---------------------- \n Repartitioning with " + partitioner.name + ", partitions: "+part +" index: "+index)
                val repmsg = definer.repartitionIndexData(RepartitionMessage(name, numberOfPartitions = part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner))
                name = repmsg.message

                //Free query to cache index
                val nnq = Some(randomQueryMessage(dim, 0.0))
                searcherBlocking.doQuery(QueryMessage(nnq = nnq, from = Some(FromMessage(FromMessage.Source.Index(name)))))
                System.out.println("Calculating no-skip queries")
                noSkipping.get(dim).get += index -> IndexedSeq.tabulate(numQ)(el => searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(name))), nnq = Some(queries.get(dim).get(el)))))
                System.out.println("No-skip queries calculated")
                for(dropPerc <- dropPartitions){
                  super.setDropPerc(dropPerc)
                  val (avgTime, noResults, informationloss, ratio) = timeQuery(index, name, dim, part, dropPerc)
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
        val split = props.properties.get("Example_row").getOrElse("").split(":")
        if(split.length<2){
          eName = None
        }else{
          val vector = split(1).split(",")
          if(vector.size==dim){
            eName = Some(entity)
            System.out.println("Entity found - " + eName.get)
          }else None
        }
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
    //System.out.println("Index name: " + name)

    name
  }

  //TODO Log individual queries in chronos
  /**
    *
    * @param indexName
    * @param dim
    * @param part
    * @param dropPerc
    * @return avg Time, avg Result-size, avg top-k misses, avg loss of precision when comparing no skipping vs skipping
    */
  def timeQuery(index: IndexType, indexName: String, dim: Int, part: Int, dropPerc : Double): (Float, Int, Float, Float) = {
    val queryCount = numQ
    //Average over Queries
    var resSize = 0
    var time = 0l
    var queryCounter = 0
    var avgMiss = 0f
    var recallLoss = 0f
    while (queryCounter < queryCount) {
      //Skipping Query
      val start = System.currentTimeMillis()
      val nnq = Some(queries.get(dim).get(queryCounter))
      val skipOpt: mutable.Map[String, String] = collection.mutable.Map() ++ nnq.get.options
      skipOpt -= "skipPart"
      skipOpt += "skipPart" -> dropPerc.toString
      val qm = QueryMessage(nnq = Some(nnq.get.copy(options = skipOpt.toMap)), from = Some(FromMessage(FromMessage.Source.Index(indexName))), information = Seq(QueryMessage.InformationLevel.WITH_PROVENANCE_PARTITION_INFORMATION, QueryMessage.InformationLevel.WITH_PROVENANCE_SOURCE_INFORMATION, QueryMessage.InformationLevel.INFORMATION_FULL_TREE))
      val dropRes = searcherBlocking.doQuery(qm)
      val stop = System.currentTimeMillis()
      time+=(stop-start)

      val gtruth = truths.get(dim).get(queryCounter)

      //no skipping
      val noSkipRes = noSkipping.get(dim).get.get(index).get(queryCounter)

      //System.out.println(dropRes.responses.head.results.size+" | "+dropRes.responses.head.results.head.data.mkString(", "))
      //System.out.println(gtruth.responses.head.results.size+" | "+gtruth.responses.head.results.head.data.mkString(", "))
      //System.out.println(noSkipRes.responses.head.results.size+" | "+noSkipRes.responses.head.results.head.data.mkString(", "))

      //Comparison Code
      val ratio = errorRatio(noSkipRes, dropRes)
      //System.out.println("Error Ratio between skip and no skip: "+ratio)

      val agreements = topKMatch(gtruth, noSkipRes)

      val skipAgree = topKMatch(gtruth, dropRes)

      val indexAgree = topKMatch(dropRes, noSkipRes)
      //System.out.println("Top K Matches between skip and no skip "+ indexAgree)

      resSize += dropRes.responses.head.results.size
      queryCounter += 1
      avgMiss+=indexAgree
      recallLoss+=ratio
      super.write(time = (stop-start).toInt,noResults=dropRes.responses.head.results.size,skipNoSkipK = indexAgree,ratioK = ratio, missingKTruth = agreements*100-skipAgree*100)
    }

    (time/ queryCount.toFloat, resSize / queryCount, avgMiss/queryCount.toFloat, recallLoss/queryCount.toFloat)
  }

  def errorRatio(truth: QueryResultsMessage, guess:QueryResultsMessage) : Float = {
    var perfectMatches = 0
    val truths = truth.responses.head.results.sortBy(_.data.get("ap_distance").get.getFloatData).zipWithIndex
    val guesses = guess.responses.head.results.sortBy(_.data.get("ap_distance").get.getFloatData).zipWithIndex.toArray
    if(guesses.size==0 || guesses.size<k){
      System.err.println("You have given " + guesses.size+ " guesses to the errorRatio method.")
    }
    val errors: Seq[Float] = truths.map(el => {
      if(el._2>=k) 0f else {
        if(el._1.data.get("ap_distance").get.getFloatData == 0f){
          perfectMatches+=1
          (guesses(el._2)._1.data.get("ap_distance").get.getFloatData+1)/(el._1.data.get("ap_distance").get.getFloatData+1)
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
      System.err.println(perfectMatches+" perfect Matches. There might be a problem in the code")
    }
    var err = 0f
    for(f <- errors){
      if(err!=Double.NaN){
        err+=f
      }else err+=1f
    }
    err/Math.min(k, guesses.size).toFloat
  }

  def topKMatch(truth: QueryResultsMessage, guess: QueryResultsMessage) : Float = {
    val truths = truth.responses.head.results.sortBy(_.data.get("ap_distance").get.getFloatData)

    val gtruthPKs = truths.take(k).map(_.data.get("pk"))
    val guessPKs = guess.responses.head.results.map(_.data.get("pk"))

    val ag= gtruthPKs.intersect(guessPKs).length
    ag.toFloat/gtruthPKs.size.toFloat
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
  def randomQueryMessage(dim: Int, skip: Double) = NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dim)(Random.nextFloat())))), None, getDistanceMsg, k, Map[String, String]("skipPart" -> skip.toString), indexOnly = indexOnly)

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