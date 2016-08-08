package ch.unibas.dmi.dbis.adam.evaluation.grpc

import java.io.File

import ch.unibas.dmi.dbis.adam.evaluation.io.{FeatureVectorIO, QueryResultIO}
import ch.unibas.dmi.dbis.adam.evaluation.utils.{AdamParEvalUtils, EvaluationResultLogger}
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc.RepartitionMessage.Partitioner
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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
  val tupleSizes = Seq(1e5.toInt)
  val dimensions = Seq(20)
  val partitions = Seq(10, 20)
  //Don't do 1K here it crashes on return because java.lang.OutOfMemoryError: GC overhead limit exceeded TODO Maybe this changes with the new commit?
  val indices = Seq(IndexType.sh, IndexType.vaf, IndexType.ecp, IndexType.lsh, IndexType.pq)
  val partitioners = Seq(RepartitionMessage.Partitioner.CURRENT, RepartitionMessage.Partitioner.ECP, RepartitionMessage.Partitioner.SPARK)

  var dropPartitions = Seq(0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.9)

  //Generate Queries
  dimensions.map(dim => {
    getOrGenQueries(dim)
  })
  var eName = ""

  //dropAllEntities()
  try
      for (tuples <- tupleSizes) {
        super.setTuples(tuples)
        for (dim <- dimensions) {
          super.setDimensions(dim)
          System.out.println(" \n ------------------ \n New Round! " + tuples + " " + dim)

          eName = getOrGenEntity(tuples, dim)
          for (index <- indices) {
            getOrGenIndex(index, eName)
          }
          getOrGenTruth(dim)

          for (index <- indices) {
            super.setIndex(index.name)
            var name = getOrGenIndex(index, eName)
            for (part <- partitions) {
              super.setPartitions(part)
              for (partitioner <- partitioners) {
                super.setPartitioner(partitioner)
                System.out.println("\n ---------------------- \n Repartitioning with " + partitioner.name + ", partitions: " + part + " index: " + index)
                val repmsg = definer.repartitionIndexData(RepartitionMessage(name, numberOfPartitions = part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner))
                name = repmsg.message

                //Free query to cache index
                val nnq = Some(randomQueryMessage(dim, 0.0))
                searcherBlocking.doQuery(QueryMessage(nnq = nnq, from = Some(FromMessage(FromMessage.Source.Index(name)))))

                for (dropPerc <- dropPartitions) {
                  super.setDropPerc(dropPerc)
                  timeQuery(index, name, dim, part, dropPerc)
                }
              }
            }
          }
        }
      }
  finally out.close()

  def getOrGenQueries(dim: Int): IndexedSeq[NearestNeighbourQueryMessage] = {
    val file = new File("resources/queries_" + dim + ".qlist")
    if (!file.exists()) {
      val queries = IndexedSeq.fill(numQ)(randomQueryMessage(dim, 0.0))
      FeatureVectorIO.writeToFile(new File("resources/queries_" + dim + ".qlist"), queries.map(nnq => nnq.query.get.getDenseVector.vector.toIndexedSeq))
      queries
    } else {
      val queries = FeatureVectorIO.fromFile(file)
      queries.map(vec => {
        NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(vec))), None, getDistanceMsg, k, Map[String, String](), indexOnly = indexOnly)
      })
    }
  }

  def getOrGenNoSkip(dim: Int, index: IndexType, part: Int, partitioner: Partitioner): IndexedSeq[Seq[QueryResultTupleMessage]] = {
    val file = new File("resources/noskip_" + dim + "_" + index + "_" + part + "_" + partitioner + ".reslist")
    if (!file.exists()) {
      System.out.println("Generating No-Skip results")
      val name = getOrGenIndex(index, eName)
      val queries = getOrGenQueries(dim)
      val res = IndexedSeq.tabulate(numQ)(el => {
        searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(name))), nnq = Some(queries(el)))).responses.head.results
      })
      QueryResultIO.writeToFile(file, res)
      System.out.println("No-skip results generated")
      res
    } else QueryResultIO.fromFile(file)
  }

  def getOrGenTruth(dim: Int): IndexedSeq[Seq[QueryResultTupleMessage]] = {
    val file = new File("resources/truths_" + dim + ".reslist")
    if (file.exists()) {
      QueryResultIO.fromFile(file)
    } else {
      System.out.println("Generating Truth")
      val queries = getOrGenQueries(dim)
      val res = IndexedSeq.tabulate(numQ)(el =>
        searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(getOrGenIndex(IndexType.vaf, eName))))
          , nnq = Some(queries(el).withIndexOnly(false))))
          .responses.head.results)
      val lb = ListBuffer[Seq[QueryResultTupleMessage]]()
      res.foreach(q => q.sliding(10000).foreach(lb += _.sortBy(_.data("ap_distance").getFloatData).take(k)))
      val topk = lb.map(_.sortBy(el => el.data("ap_distance").getFloatData).take(k)).toIndexedSeq
      QueryResultIO.writeToFile(file, topk)
      System.out.println("VA-Scans executed")
      res
    }
  }

  /** Checks if an Entity with the given Tuple size and dimensions exists */
  def getOrGenEntity(tuples: Int, dim: Int): String = {
    var eName: Option[String] = None
    val entitymessage = definer.listEntities(EmptyMessage())

    for (entity <- entitymessage.entities) {
      val c = definer.count(EntityNameMessage(entity))
      if (c.message.toInt == tuples) {
        val props = definer.getEntityProperties(EntityNameMessage(entity))
        System.out.println(props)
        val split = props.properties.getOrElse("Example_row", "").split(":")
        if (split.length < 2) {
          eName = None
        } else {
          val vector = split(1).split(",")
          if (vector.size == dim) {
            eName = Some(entity)
            System.out.println("Entity found - " + eName.get)
          } else None
        }
      }
    }

    if (eName.isEmpty) {
      System.out.println("Generating new Entity")
      eName = Some(("silvan" + Math.abs(Random.nextInt())).filter(_ != '0'))
      definer.createEntity(CreateEntityMessage(eName.get, Seq(AttributeDefinitionMessage.apply("pk", AttributeType.LONG, pk = true, unique = true, indexed = true), AttributeDefinitionMessage("feature", AttributeType.FEATURE, pk = false, unique = false, indexed = true))))
      val options = Map("fv-dimensions" -> dim, "fv-min" -> 0, "fv-max" -> 1, "fv-sparse" -> false).mapValues(_.toString)
      definer.generateRandomData(GenerateRandomDataMessage(eName.get, tuples, options))
    }

    eName.get
  }

  /** Loads truths & noSkips from Filesystem and writes to quality-logger */
  def timeQuery(index: IndexType, indexName: String, dim: Int, part: Int, dropPerc: Double): Unit = {
    val queries = getOrGenQueries(dim)
    val truths = getOrGenTruth(dim)
    val noSkipping = getOrGenNoSkip(dim, index, part, super.getPartitioner)

    val queryCount = numQ

    var time = 0l
    var queryCounter = 0
    while (queryCounter < queryCount) {
      //Skipping Query
      val start = System.currentTimeMillis()
      val nnq = Some(queries(queryCounter))
      val skipOpt: mutable.Map[String, String] = collection.mutable.Map() ++ nnq.get.options
      skipOpt -= "skipPart"
      skipOpt += "skipPart" -> dropPerc.toString

      val qm = QueryMessage(nnq = Some(nnq.get.withOptions(skipOpt.toMap).withIndexOnly(indexOnly)),
        from = Some(FromMessage(FromMessage.Source.Index(indexName))),
        information = Seq())

      val dropRes = searcherBlocking.doQuery(qm).responses.head.results
      val stop = System.currentTimeMillis()
      time += (stop - start)

      //Comparison Code
      val gtruth = truths(queryCounter)
      val noSkipRes = noSkipping(queryCounter)
      val ratio = errorRatio(noSkipRes, dropRes)
      val agreements = topKMatch(gtruth, noSkipRes)
      val skipAgree = topKMatch(gtruth, dropRes)

      super.write(time = (stop - start).toInt, noResults = dropRes.size, ratioK = ratio,
        missingKTruth = agreements * 100 - skipAgree * 100, topKNoSkip = agreements)
      queryCounter += 1
    }
  }

  /** Compares ap_distance of top k tuples. TODO This should be rewritten for the case t1=0 t2=n. */
  def errorRatio(truth: Seq[QueryResultTupleMessage], guess: Seq[QueryResultTupleMessage]): Float = {
    var perfectMatches = 0
    val truths = truth.sortBy(_.data("ap_distance").getFloatData).zipWithIndex
    val guesses = guess.sortBy(_.data("ap_distance").getFloatData).zipWithIndex.toArray
    if (guesses.length == 0 || guesses.length < k) {
      System.err.println("You have given " + guesses.length + " guesses to the errorRatio method.")
    }
    val errors: Seq[Float] = truths.map(el => {
      if (el._2 >= k) 0f
      else {
        if (el._1.data("ap_distance").getFloatData == 0f) {
          perfectMatches += 1
          (guesses(el._2)._1.data("ap_distance").getFloatData + 1) / (el._1.data("ap_distance").getFloatData + 1)
        } else {
          if (el._2 >= guesses.length) 0f else guesses(el._2)._1.data("ap_distance").getFloatData / el._1.data("ap_distance").getFloatData
        }
      }
    })
    var err = 0f
    for (f <- errors) {
      if (err != Double.NaN) {
        err += f
      } else err += 1f
    }
    err / Math.min(k, Math.max(1, guesses.length)).toFloat  //sanity-check for division by 0
  }

  /** Simple top-K intersection count */
  def topKMatch(truth: Seq[QueryResultTupleMessage], guess: Seq[QueryResultTupleMessage]): Float = {
    val gtruthPKs = truth.map(_.data.get("pk"))
    val guessPKs = guess.map(_.data.get("pk"))

    val ag = gtruthPKs.intersect(guessPKs).length
    ag.toFloat / Math.max(1, gtruthPKs.size.toFloat)  //sanity-check for division by 0
  }

  /** Information about partitions */
  def printPartInfo(res: QueryResultsMessage): Unit = {
    val partInfo = mutable.HashMap[Int, Int]()
    res.responses.foreach(f => f.results.foreach(r => {
      val key = r.data.getOrElse("ap_partition", DataMessage.defaultInstance).getIntData
      val value = partInfo.getOrElse(key, 0)
      partInfo.put(key, value + 1)
      System.out.println("Query done ,Partition Info: " + partInfo.toString())
      System.out.println("Sample response: " + res.responses.head.results.head.data.mkString(", "))

      val sorted = res.responses.head.results.sortBy(f => f.data("ap_distance").getFloatData)

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
  def randomQueryMessage(dim: Int, skip: Double): NearestNeighbourQueryMessage = NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dim)(Random.nextFloat())))), None, getDistanceMsg, k, Map[String, String]("skipPart" -> skip.toString), indexOnly = indexOnly)

  /** Drops all entities. Careful when using this operation*/
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


  /** Checks if Index exists and generates it otherwise */
  def getOrGenIndex(index: IndexType, eName: String): String = {
    val indexList = definer.listIndexes(EntityNameMessage(eName))
    var name = ""
    if (!indexList.indexes.exists(el => el.indextype == index)) {
      System.out.println("Index " + index.name + " does not exist, generating... ")
      name = generateIndex(index, eName)
    } else name = indexList.indexes.find(im => im.indextype == index).get.index
    name
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