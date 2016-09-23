package ch.unibas.dmi.dbis.adam.evaluation.grpc

import java.io.File
import java.util.Scanner

import ch.unibas.dmi.dbis.adam.evaluation.io.SeqIO
import ch.unibas.dmi.dbis.adam.evaluation.utils._
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.{ManagedChannel, ManagedChannelBuilder}
import org.vitrivr.adam.grpc.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import org.vitrivr.adam.grpc.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import org.vitrivr.adam.grpc.grpc.RepartitionMessage.Partitioner
import org.vitrivr.adam.grpc.grpc._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub, host: String) extends AdamParEvalUtils with Logging {

  val querypath = "evaluation/src/main/resources/sift_query.fvecs"
  val truthpath = "evaluation/src/main/resources/sift_groundtruth.ivecs"

  val k = 100
  val pr = 101

  val compareK = true

  /**
    * Evaluation Params
    */
  val indexOnly = false
  val numQ = 10
  val tupleSizes = Seq(1e6.toInt)
  val dimensions = Seq(128)
  val partitions = Seq(10, 20, 50)
  val indices = Seq(IndexType.vaf, IndexType.sh, IndexType.ecp, IndexType.lsh)
  val indicesToGenerate = Seq(IndexType.vaf, IndexType.sh, IndexType.ecp, IndexType.lsh)

  val partitioners = Seq(RepartitionMessage.Partitioner.ECP, RepartitionMessage.Partitioner.SPARK, RepartitionMessage.Partitioner.SH)

  var dropPartitions = Seq(0.0, 0.1, 0.2, 0.3, 0.4, 0.5)
  var eName = "sift_realdata"
  val information = collection.mutable.Map[String, Any]()
  information.put("k", k)
  PartitionResultLogger.init
  EvaluationResultLogger.init(pr)
  //dropAllEntities()
  //definer.dropEntity(EntityNameMessage("sil_" + 1e6.toInt + "_" + 128 + "_" + host.replace(".", "")))
  //definer.dropIndex(IndexNameMessage(getOrGenIndex(IndexType.ecp, "sift_realdata")))

  for (tuples <- tupleSizes) {
    information.put("tuples", tuples)
    for (dim <- dimensions) {
      information.put("dimensions", dim)
      eName = getOrGenEntity(tuples, dim)
      for (index <- indicesToGenerate) {
        getOrGenIndex(index, eName)
      }
      getOrGenQueries(dim)
      getOrGenTruth(dim)

      for (index <- indices) {
        information.put("index", index)
        var name = getOrGenIndex(index, eName)
        for (part <- partitions) {
          information.put("partitions", part)
          for (partitioner <- partitioners) {
            information.put("partitioner", partitioner)
            var repartition = definer.repartitionIndexData(RepartitionMessage(name, numberOfPartitions = part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner))
            if (repartition.code != AckMessage.Code.OK) {
              log.error("Failed repartitioning {} for {} partitions with {}", index.name, part.toString, partitioner.name)
              Thread.sleep(2000)
              while (definer.count(EntityNameMessage(eName)).code != AckMessage.Code.OK) {
                log.error("Connection failed... Reconnecting")
                Thread.sleep(10000)
              }
              log.debug("Reconnected")
              repartition = definer.repartitionIndexData(RepartitionMessage(name, numberOfPartitions = part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner))
            }
            if (repartition.code == AckMessage.Code.OK) {
              name = repartition.message

              val props = definer.getEntityProperties(EntityNameMessage(name))
              PartitionResultLogger.write(information + ("distribution" -> props.properties("tuplesPerPartition")) toMap)
              //Free query to cache index
              val nnq = Some(randomQueryMessage(dim, 0.0))
              searcherBlocking.doQuery(QueryMessage(nnq = nnq, from = Some(FromMessage(FromMessage.Source.Index(name)))))

              log.debug("Timing queries")
              for (dropPerc <- dropPartitions) {
                timeQuery(name, dropPerc)
              }
            }
          }
        }
      }
    }
  }
  log.info("I'm done")


  def getOrGenQueries(dim: Int): IndexedSeq[NearestNeighbourQueryMessage] = {
    if (eName.equals("sift_realdata")) {
      val queries = SIFTQueries.getQueries(querypath, numQ)
      return queries.map(vec => {
        NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(vec))), None, getDistanceMsg, k, Map[String, String](), indexOnly = indexOnly)
      })
    }
    val file = new File("resources/" + eName + "/queries_" + dim + ".qlist")
    if (!file.exists()) {
      log.debug("Generating Queries for " + dim + " Dimensions")
      val queries = IndexedSeq.fill(numQ)(randomQueryMessage(dim, 0.0))
      SeqIO.storeNestedSeq(file, queries.map(nnq => nnq.query.get.getDenseVector.vector.toIndexedSeq))
      queries
    } else {
      val queries = SeqIO.readNestedSeq(file)
      queries.map(vec => {
        NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(vec))), None, getDistanceMsg, k, Map[String, String](), indexOnly = indexOnly)
      })
    }
  }

  /** We only store top-k matches */
  def getOrGenNoSkip(dim: Int, index: IndexType, part: Int, partitioner: Partitioner): IndexedSeq[Float] = {
    if (!compareK) return IndexedSeq.fill(numQ)(1f)
    val file = new File("resources/" + eName + "/noskip_" + dim + "_" + index + "_" + part + "_" + partitioner + ".reslist")
    if (!file.exists()) {
      log.debug("Generating No-Skip results for " + dim + ", " + index + ", " + part + ", " + partitioner)
      val name = getOrGenIndex(index, eName)
      val queries = getOrGenQueries(dim)
      val truths = getOrGenTruth(dim)
      //While-Loop because performance and memory
      val lb = ListBuffer[Float]()
      var counter = 0
      while (counter < queries.size) {
        val res = searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(name))), nnq = Some(queries(counter)))).responses.head.results
        val topk = topKMatch(truths(counter), res)
        lb += topk
        counter += 1
        log.debug(counter + "/" + queries.size)
      }
      val res = lb.toIndexedSeq
      SeqIO.storeSeq(file, res)
      res
    } else SeqIO.readSeq(file)
  }

  /**
    * Returns the ordered list of correct pks for all queries
    */
  def getOrGenTruth(dim: Int): IndexedSeq[IndexedSeq[Float]] = {
    val distances = new File("resources/" + eName + "/truths_" + dim + ".apdists")
    if (eName.equals("sift_realdata") && distances.exists()) {
      val truths = SIFTQueries.getTruths(truthpath, numQ).map(_.map(_.toFloat))
      return truths
    }

    val file = new File("resources/" + eName + "/truths_" + dim + ".reslist")
    if (file.exists()) {
      SeqIO.readNestedSeq(file)
    } else {
      val queries = getOrGenQueries(dim)
      log.debug("Generating Truth for " + dim + " dimensions")

      var qCounter = 0
      val pks = ListBuffer[IndexedSeq[Float]]()
      val dists = ListBuffer[IndexedSeq[Float]]()
      while (qCounter < numQ) {
        val t1 = System.currentTimeMillis()
        val qres = searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(getOrGenIndex(IndexType.vaf, eName))))
          , nnq = Some(queries(qCounter).withIndexOnly(false))))
          .responses.head.results
        val t2 = System.currentTimeMillis()
        pks += qres.map(t => (t.data("ap_distance").getFloatData, t.data("pk").getLongData.toFloat)).sortBy(_._1).take(k).map(_._2).toIndexedSeq
        dists += qres.map(t => t.data("ap_distance").getFloatData).sortBy(el => el).take(k).toIndexedSeq
        qCounter += 1
        log.debug(qCounter + "/" + numQ + ", time: " + (t2 - t1))
      }
      val topk = pks.toIndexedSeq
      SeqIO.storeNestedSeq(distances, dists.toIndexedSeq)
      SeqIO.storeNestedSeq(file, topk)
      if(eName.equals("sift_realdata")){
        SIFTQueries.getTruths(truthpath, numQ).map(_.map(_.toFloat))
      }else{
        topk
      }
    }
  }

  //List of Ap_distances
  def getOrGenFullTruth(dim: Int) : IndexedSeq[IndexedSeq[Float]] = {
    val file = new File("resources/" + eName + "/truths_" + dim + ".apdists")
    getOrGenTruth(dim)
    SeqIO.readNestedSeq(file)
  }

  /** Checks if an Entity with the given Tuple size and dimensions exists */
  def getOrGenEntity(tuples: Int, dim: Int): String = {
    if (tuples == 1e6.toInt && dim == 128) {
      return "sift_realdata"
    }
    val eName = "sil_" + tuples + "_" + dim + "_" + host.replace(".", "")
    val exists = definer.listEntities(EmptyMessage()).entities.find(_.equals(eName))
    if (exists.isEmpty) {
      log.info("Generating new Entity: " + eName)
      definer.createEntity(CreateEntityMessage(eName, Seq(AttributeDefinitionMessage.apply("pk", AttributeType.LONG, pk = true, unique = true, indexed = true),
        AttributeDefinitionMessage("feature", AttributeType.FEATURE, pk = false, unique = false, indexed = true))))
      val options = Map("fv-dimensions" -> dim, "fv-min" -> 0, "fv-max" -> 1, "fv-sparse" -> false).mapValues(_.toString)
      definer.generateRandomData(GenerateRandomDataMessage(eName, tuples, options))
    } else log.info("Using existing entity: " + eName)
    eName
  }

  /** Loads truths & noSkips from Filesystem and writes to quality-logger */
  def timeQuery(indexName: String, dropPerc: Double): Unit = {
    val dim = information("dimensions").toString.toInt
    val index = information("index").asInstanceOf[IndexType]
    val part = information("partitions").toString.toInt
    val queries = getOrGenQueries(dim)
    val truths = getOrGenTruth(dim)
    val distances = getOrGenFullTruth(dim)
    val topKRes = getOrGenNoSkip(dim, index, part, information("partitioner").asInstanceOf[Partitioner])

    val queryCount = numQ

    var queryCounter = 0
    while (queryCounter < queryCount) {
      //Skipping Query
      val nnq = Some(queries(queryCounter))
      val skipOpt: mutable.Map[String, String] = collection.mutable.Map() ++ nnq.get.options
      skipOpt -= "skipPart"
      skipOpt += "skipPart" -> dropPerc.toString

      val qm = QueryMessage(nnq = Some(nnq.get.withOptions(skipOpt.toMap).withIndexOnly(indexOnly)),
        from = Some(FromMessage(FromMessage.Source.Index(indexName))),
        information = Seq())
      val start = System.currentTimeMillis()
      val dropRes = searcherBlocking.doQuery(qm).responses.head.results
      val stop = System.currentTimeMillis()

      //Comparison Code
      val gtruth = truths(queryCounter)
      val noskipRecall = topKRes(queryCounter)
      val skipRecall = recall(gtruth, dropRes, k)
      //TODO Inefficient
      val quality : Float = qualityError(distances(queryCounter), dropRes.toIndexedSeq)
      val approx = approxError(distances(queryCounter),dropRes)

      val prValues = IndexedSeq.tabulate(pr)(el => (precision(gtruth, dropRes, el), recall(gtruth, dropRes, el)))
      val res = information ++ Map("time" -> (stop - start), "nores" -> dropRes.size, "skip_recall" -> skipRecall, "noskip_recall" -> noskipRecall, "skipPercentage" -> dropPerc, "quality"->quality, "errorApprox" -> approx)
      EvaluationResultLogger.writePR(res toMap, prValues)
      queryCounter += 1
    }
  }


  def qualityError(truth: IndexedSeq[Float], guess: IndexedSeq[QueryResultTupleMessage]): Float = {
    if(guess.size<k){
      log.error(guess.size+"guess size too small. Information:  "+EvaluationResultLogger.getLast.mkString(":"))
      return -1
    }
    val sorted = guess.sortBy(_.data("ap_distance").getFloatData)
    truth.take(k).zipWithIndex.map(el => sorted(el._2).data("ap_distance").getFloatData/truth(el._2)).sum / Math.max(1, Math.min(k, truth.size))
  }

  def approxError(truth: IndexedSeq[Float], guess: Seq[QueryResultTupleMessage]): Float = {
    val sorted = guess.sortBy(_.data("ap_distance").getFloatData)

    val _res= {
      val el = sorted.take(k).map(_.data("ap_distance").getFloatData).sum
      if(el==0){
        1
      }else el
    }/
    truth.take(k).sum
    _res
  }

  /** Recall @ k */
  def recall(truth: IndexedSeq[Float], guess: Seq[QueryResultTupleMessage], k: Int): Float = {
    val guessPKs = guess.sortBy(el => el.data("ap_distance").getFloatData).map(_.data("pk").getLongData.toFloat)
    val ag = truth.take(k).intersect(guessPKs.take(k)).length
    ag.toFloat / Math.max(1, truth.size) //sanity-check for division and if k > truth.size
  }

  /** Precision @ k */
  def precision(truth: IndexedSeq[Float], guess: Seq[QueryResultTupleMessage], k: Int): Float = {
    val guessPKs = guess.sortBy(el => el.data("ap_distance").getFloatData).map(_.data("pk").getLongData.toFloat)
    val ag = truth.take(k).intersect(guessPKs.take(k)).length
    ag.toFloat / k
  }


  /** Top-K intersection count normalized between 0 and 1 */
  def topKMatch(truth: IndexedSeq[Float], guess: Seq[QueryResultTupleMessage]): Float = {
    val guessPKs = guess.map(_.data("pk").getLongData.toFloat)
    val ag = truth.intersect(guessPKs).length
    ag.toFloat / Math.max(1, truth.size.toFloat) //sanity-check for division by 0
  }

  /** Generates a random query using Random.nextFloat() */
  def randomQueryMessage(dim: Int, skip: Double): NearestNeighbourQueryMessage = NearestNeighbourQueryMessage("feature", Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dim)(Random.nextFloat())))), None, getDistanceMsg, k, Map[String, String]("skipPart" -> skip.toString), indexOnly = indexOnly)

  /** Drops all entities. Careful when using this operation */
  def dropAllEntities() = {

    log.warn("You have requested to drop all entities on host: " + host + ". Please confirm by entering the number 42.")
    val keyboard = new Scanner(System.in)
    val myint = keyboard.nextInt()
    if (myint != 42) {
      System.exit(1)
    }
    val entityList = definer.listEntities(EmptyMessage())

    for (entity <- entityList.entities) {
      log.debug("Dropping Entity: " + entity)
      try {
        val props = definer.getEntityProperties(EntityNameMessage(entity))
        log.debug(props.properties.mkString(":"))
      }
      catch {
        case e: Exception => System.err.println("Error in reading properties")
      }
      val dropEnt = definer.dropEntity(EntityNameMessage(entity))
      if (dropEnt.code.isError) {
        log.error("Error when dropping Entity " + entity + ": " + dropEnt.message)
      }
      log.debug("Dropped Entity " + entity)
      val file = new File("resources/" + entity)
      for (list <- Option(file.listFiles()); child <- list) child.delete()
    }
  }

  /** Generates DistanceMessage with Minkowski-norm 2 */
  def getDistanceMsg: Option[DistanceMessage] = Some(DistanceMessage(DistanceMessage.DistanceType.minkowski, Map[String, String](("norm", "2"))))

  /** Checks if Index exists and generates it otherwise */
  def getOrGenIndex(index: IndexType, eName: String): String = {
    val indexList = definer.listIndexes(EntityNameMessage(eName))
    var name = ""
    if (!indexList.indexes.exists(el => el.indextype == index)) {
      log.info("Index " + index.name + " does not exist, generating... ")
      name = {
        val indexMsg = IndexMessage(eName, "feature", index, getDistanceMsg, Map[String, String]())
        definer.index(indexMsg).message
      }
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
      AdamSearchGrpc.stub(channel), host
    )
  }
}