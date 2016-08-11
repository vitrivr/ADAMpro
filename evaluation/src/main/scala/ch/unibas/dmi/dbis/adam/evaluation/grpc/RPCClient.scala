package ch.unibas.dmi.dbis.adam.evaluation.grpc

import java.io.File

import ch.unibas.dmi.dbis.adam.evaluation.io.SeqIO
import ch.unibas.dmi.dbis.adam.evaluation.utils.{AdamParEvalUtils, EvaluationResultLogger, Logging}
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
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) extends EvaluationResultLogger with AdamParEvalUtils with Logging {

  val k = 200
  super.setK(k)

  val compareK = false

  /**
    * Evaluation Params
    */
  val indexOnly = true
  val numQ = 5
  val tupleSizes = Seq(1e6.toInt)
  val dimensions = Seq(20, 64, 128)
  val partitions = Seq(10, 20, 50)
  val indices = Seq(IndexType.vaf)
  val indicesToGenerate = Seq(IndexType.vaf, IndexType.sh, IndexType.ecp)
  val partitioners = Seq(RepartitionMessage.Partitioner.CURRENT, RepartitionMessage.Partitioner.ECP, RepartitionMessage.Partitioner.SPARK)

  var dropPartitions = Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.9)
  var eName = ""
  //dropAllEntities()

  try
      for (tuples <- tupleSizes) {
        super.setTuples(tuples)
        for (dim <- dimensions) {
          super.setDimensions(dim)
          eName = getOrGenEntity(tuples, dim)
          for (index <- indicesToGenerate) {
            getOrGenIndex(index, eName)
          }
          getOrGenQueries(dim)
          getOrGenTruth(dim)
          System.gc()

          for (index <- indices) {
            super.setIndex(index.name)
            var name = getOrGenIndex(index, eName)
            for (part <- partitions) {
              super.setPartitions(part)
              for (partitioner <- partitioners) {
                super.setPartitioner(partitioner)
                name = definer.repartitionIndexData(RepartitionMessage(name, numberOfPartitions = part, option = RepartitionMessage.PartitionOptions.REPLACE_EXISTING, partitioner = partitioner)).message

                //Free query to cache index
                val nnq = Some(randomQueryMessage(dim, 0.0))
                searcherBlocking.doQuery(QueryMessage(nnq = nnq, from = Some(FromMessage(FromMessage.Source.Index(name)))))

                log.debug("Timing queries")
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
  log.debug("I'm done")

  def getOrGenQueries(dim: Int): IndexedSeq[NearestNeighbourQueryMessage] = {
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
    if(!compareK) return IndexedSeq.fill(numQ)(1f)
    val file = new File("resources/" + eName + "/noskip_" + dim + "_" + index + "_" + part + "_" + partitioner + ".reslist")
    if (!file.exists()) {
      log.debug("Generating No-Skip results for "+dim+", "+index+", "+part+", "+partitioner)
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
        log.debug(counter+"/"+queries.size)
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
    val file = new File("resources/" + eName + "/truths_" + dim + ".reslist")
    if (file.exists()) {
      SeqIO.readNestedSeq(file)
    } else {
      System.gc()
      val queries = getOrGenQueries(dim)
      log.debug("Generating Truth for " + dim + " dimensions")

      //Switching to Counter because scala
      var qCounter = 0
      val pks = ListBuffer[IndexedSeq[Float]]()
      while(qCounter<numQ){
        val qres = searcherBlocking.doQuery(QueryMessage(from = Some(FromMessage(FromMessage.Source.Index(getOrGenIndex(IndexType.vaf, eName))))
          , nnq = Some(queries(qCounter).withIndexOnly(false))))
          .responses.head.results
        pks+=qres.map(t => (t.data("ap_distance").getFloatData, t.data("pk").getLongData.toFloat)).sortBy(_._1).take(k).map(_._2).toIndexedSeq
        qCounter+=1
        log.debug(qCounter+"/"+numQ)
      }
      val topk = pks.toIndexedSeq

      SeqIO.storeNestedSeq(file, topk)
      topk
    }
  }

  /** Checks if an Entity with the given Tuple size and dimensions exists */
  def getOrGenEntity(tuples: Int, dim: Int): String = {
    val eName = "sil_" + tuples + "_" + dim
    val exists = definer.listEntities(EmptyMessage()).entities.find(_.equals(eName))
    if (exists.isEmpty) {
      log.info("Generating new Entity: " + eName)
      definer.createEntity(CreateEntityMessage(eName, Seq(AttributeDefinitionMessage.apply("pk", AttributeType.LONG, pk = true, unique = true, indexed = true),
        AttributeDefinitionMessage("feature", AttributeType.FEATURE, pk = false, unique = false, indexed = true))))
      val options = Map("fv-dimensions" -> dim, "fv-min" -> 0, "fv-max" -> 1, "fv-sparse" -> false).mapValues(_.toString)
      definer.generateRandomData(GenerateRandomDataMessage(eName, tuples, options))
    }
    eName
  }

  /** Loads truths & noSkips from Filesystem and writes to quality-logger */
  def timeQuery(index: IndexType, indexName: String, dim: Int, part: Int, dropPerc: Double): Unit = {
    val queries = getOrGenQueries(dim)
    val truths = getOrGenTruth(dim)
    val topKRes = getOrGenNoSkip(dim, index, part, super.getPartitioner)

    val queryCount = numQ

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

      //Comparison Code
      val gtruth = truths(queryCounter)
      val agreements = topKRes(queryCounter)
      val skipAgree = topKMatch(gtruth, dropRes)

      super.write(time = (stop - start).toInt, noResults = dropRes.size,
        topKSkip = skipAgree, topKNoSkip = agreements)
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
    err / Math.min(k, Math.max(1, guesses.length)).toFloat //sanity-check for division by 0
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
      val file = new File("resources/"+entity)
      for(list <- Option(file.listFiles()) ;  child <- list) child.delete()
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
      AdamSearchGrpc.stub(channel)
    )
  }
}