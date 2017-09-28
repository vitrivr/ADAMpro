package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties
import java.util.logging.Logger

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.Helpers
import org.vitrivr.adampro.communication.RPCClient
import org.vitrivr.adampro.communication.datastructures._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.{Random, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
abstract class Executor(val job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) {
  val NFIRST_RUN_QUERIES = 3

  val logger: Logger = Logger.getLogger(this.getClass.getName)

  //rpc client
  val client: RPCClient = RPCClient(job.adampro_url, job.adampro_port)

  //if job has been aborted, running will be set to false so that no new queries are started
  var running = true
  private var progress = 0.0

  protected val FEATURE_VECTOR_ATTRIBUTENAME = "vector"

  /**
    *
    * @return
    */
  def run(): Properties

  /**
    *
    * @return
    */
  def updateStatus(progress: Double) = {
    this.progress = progress
    setStatus(this.progress)
  }

  /**
    *
    * @return
    */
  def getStatus = progress


  /**
    * Returns combinations of partitionings.
    *
    * @return
    */
  protected def getPartitionCombinations(): Seq[(Option[Int], Option[Int])] = {
    val entityPartitions = if (job.access_entity_partitions.length > 0) {
      job.access_entity_partitions.map(Some(_))
    } else {
      Seq(None)
    }

    val indexPartitions = if (job.execution_name != "sequential" && job.access_index_partitions.length > 0) {
      job.access_index_partitions.map(Some(_))
    } else {
      Seq(None)
    }

    //cartesian product
    for {e <- entityPartitions; i <- indexPartitions} yield (e, i)
  }


  /**
    * Gets queries.
    *
    * @return
    */
  protected def getQueries(entityname: String, options: Seq[(String, String)] = Seq()): Seq[RPCGenericQueryObject] = {
    val lb = new ListBuffer[RPCGenericQueryObject]()

    val additionals = if (job.measurement_firstrun) {
      NFIRST_RUN_QUERIES
    } else {
      0
    }

    job.query_k.flatMap { k =>
      val denseQueries = (0 until (job.query_n + additionals)).map { i => getQuery(entityname, k, false, options) }

      denseQueries
    }
  }


  /**
    * Gets single query
    *
    * @param entityname
    * @param k
    * @param sparseQuery
    * @param options
    * @return
    */
  protected def getQuery(entityname: String, k: Int, sparseQuery: Boolean, options: Seq[(String, String)] = Seq()): RPCGenericQueryObject = {
    val id = Helpers.generateString(10)

    val lb = new ListBuffer[(String, String)]()

    lb.append("entityname" -> entityname)

    //lb.append("projection" -> "ap_id,ap_distance")

    lb.append("attribute" -> job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME))

    lb.append("k" -> k.toString)

    lb.append("distance" -> job.query_distance)

    if (job.query_weighted) {
      lb.append("weights" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))
    }

    if (job.query_path.isDefined) {
      lb.append("query" -> getFeatureVector(job.query_path.get).mkString(","))
    } else {
      lb.append("query" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))
    }

    if (sparseQuery) {
      lb.append("sparsequery" -> "true")
    }

    if (!job.execution_withsequential) {
      lb.append("indexonly" -> "true")
    }

    lb.append("informationlevel" -> "minimal")

    lb.append("hints" -> job.execution_hint)

    if (job.execution_subtype != null && job.execution_subtype.length > 0) {
      lb.append("subtype" -> job.execution_subtype)
    }

    lb.append(("nofallback" -> "true"))

    if (job.execution_hint == "sequential") {
      RPCSequentialScanQueryObject(id, (options ++ lb).toMap)
    } else {
      //index scan
      RPCIndexScanQueryObject(id, (options ++ lb).toMap)
    }
  }


  /**
    * Generates a feature vector.
    *
    * @param dimensions
    * @param sparsity
    * @param min
    * @param max
    * @return
    */
  protected def generateFeatureVector(dimensions: Int, sparsity: Float, min: Float, max: Float): Seq[Float] = {
    var fv: Array[Float] = (0 until dimensions).map(i => {
      var rval = Random.nextFloat * (max - min) + min
      //ensure that we do not have any zeros in vector, sparsify later
      while (math.abs(rval) < 10E-6) {
        rval = Random.nextFloat * (max - min) + min
      }

      rval
    }).toArray

    //zero the elements in the vector
    val nzeros = math.floor(dimensions * sparsity).toInt
    (0 until nzeros).map(i => Random.nextInt(dimensions)).foreach { i =>
      fv(i) = 0.toFloat
    }

    fv.toSeq
  }


  private var queryCache = mutable.Map[String, Seq[String]]()

  /**
    * Gets a feature vector.
    *
    * @param path
    * @return
    */
  protected def getFeatureVector(path: String): Seq[Float] = {
    if (!queryCache.contains(path)) {
      queryCache += path -> Source.fromFile(path).getLines.toSeq
    }


    val queries = queryCache.get(path).get
    val randomQueryIndex = Random.nextInt(queries.length)

    queries(randomQueryIndex).split(",").map(_.toFloat)
  }


  /**
    * Sparsifies a vector.
    *
    * @param vec
    * @return
    */
  protected def sparsify(vec: Seq[Float]) = {
    val ii = new ListBuffer[Int]()
    val vv = new ListBuffer[Float]()

    vec.zipWithIndex.foreach { x =>
      val v = x._1
      val i = x._2

      if (math.abs(v) > 1E-10) {
        ii.append(i)
        vv.append(v)
      }
    }

    (vv.toArray, ii.toArray, vec.size)
  }

  /**
    * Executes a query.
    *
    * @param qo
    */
  protected def executeQuery(qo: RPCGenericQueryObject): Map[String, String] = {
    val lb = new ListBuffer[(String, Any)]()

    lb ++= (job.getAllParameters())

    logger.fine("executing query with parameters: " + job.getAllParameters().mkString)

    lb += ("queryid" -> qo.id)
    lb += ("operation" -> qo.operation)
    lb += ("options" -> qo.options.mkString)
    lb += ("debugQuery" -> qo.buildQueryMessage.toString())

    val t1 = System.currentTimeMillis

    //do query
    val res: Try[Seq[RPCQueryResults]] = client.doQuery(qo)


    val t2 = System.currentTimeMillis

    lb += ("starttime" -> t1)
    lb += ("isSuccess" -> res.isSuccess)
    lb += ("nResults" -> res.map(_.length).getOrElse(0))
    lb += ("endtime" -> t2)

    if (res.isSuccess) {
      //time
      lb += ("measuredtime" -> res.get.map(_.time).mkString(";"))
      lb += ("totaltime" -> math.abs(t2 - t1).toString)

      //results
      lb += ("results" -> {
        res.get.head.results.map(res => (res.get("ap_id").getOrElse("-") + "," + res.get("ap_distance").getOrElse("-1"))).mkString("(", "),(", ")")
      })

      //result quality
      if (job.measurement_resultquality) {
        //perform sequential query
        val opt = collection.mutable.Map() ++ qo.options
        opt -= "hints"
        opt += "hints" -> "sequential"
        val gtruth = client.doQuery(RPCSequentialScanQueryObject(qo.id, opt.toMap))

        if (gtruth.isSuccess) {
          lb += ("gtresults" -> {
            res.get.head.results.map(res => (res.get("ap_id").getOrElse("-") + "," + res.get("ap_distance").getOrElse("-1"))).mkString("(", "),(", ")")
          })

          lb += ("resultquality" -> getSimpleRecall(res.get, gtruth.get, None))

          lb += ("resultquality-cr@1" -> getCompetitiveRecallAtK(res.get, gtruth.get, Some(1)))
          lb += ("resultquality-cr@10" -> getCompetitiveRecallAtK(res.get, gtruth.get, Some(10)))
          lb += ("resultquality-cr@20" -> getCompetitiveRecallAtK(res.get, gtruth.get, Some(20)))
          lb += ("resultquality-cr@50" -> getCompetitiveRecallAtK(res.get, gtruth.get, Some(50)))
          lb += ("resultquality-cr@100" -> getCompetitiveRecallAtK(res.get, gtruth.get, Some(100)))
          lb += ("resultquality-cr@" + qo.options.get("k").get -> getCompetitiveRecallAtK(res.get, gtruth.get, Some(qo.options.get("k").get.toInt)))

          lb += ("resultquality-avo" -> getAverageOverlap(res.get, gtruth.get, None))
          lb += ("resultquality-avo1" -> getAverageOverlap(res.get, gtruth.get, Some(1)))
          lb += ("resultquality-avo10" -> getAverageOverlap(res.get, gtruth.get, Some(10)))
          lb += ("resultquality-avo100" -> getAverageOverlap(res.get, gtruth.get, Some(100)))

          lb += ("resultquality-rbo0.05" -> getRBO(res.get, gtruth.get, 0.05, None))
          lb += ("resultquality-rbo0.1" -> getRBO(res.get, gtruth.get, 0.1, None))
          lb += ("resultquality-rbo0.2" -> getRBO(res.get, gtruth.get, 0.2, None))
          lb += ("resultquality-rbo0.5" -> getRBO(res.get, gtruth.get, 0.5, None))
          lb += ("resultquality-rbo0.8" -> getRBO(res.get, gtruth.get, 0.8, None))
          lb += ("resultquality-rbo0.98" -> getRBO(res.get, gtruth.get, 0.98, None))

          lb += ("resultquality-rbo0.05@10" -> getRBO(res.get, gtruth.get, 0.05, Some(10)))
          lb += ("resultquality-rbo0.1@10" -> getRBO(res.get, gtruth.get, 0.1, Some(10)))
          lb += ("resultquality-rbo0.2@10" -> getRBO(res.get, gtruth.get, 0.2, Some(10)))
          lb += ("resultquality-rbo0.5@10" -> getRBO(res.get, gtruth.get, 0.5, Some(10)))
          lb += ("resultquality-rbo0.8@10" -> getRBO(res.get, gtruth.get, 0.8, Some(10)))
          lb += ("resultquality-rbo0.98@10" -> getRBO(res.get, gtruth.get, 0.98, Some(10)))

          lb += ("resultquality-rbo0.05@20" -> getRBO(res.get, gtruth.get, 0.05, Some(20)))
          lb += ("resultquality-rbo0.1@20" -> getRBO(res.get, gtruth.get, 0.1, Some(20)))
          lb += ("resultquality-rbo0.2@20" -> getRBO(res.get, gtruth.get, 0.2, Some(20)))
          lb += ("resultquality-rbo0.5@20" -> getRBO(res.get, gtruth.get, 0.5, Some(20)))
          lb += ("resultquality-rbo0.8@20" -> getRBO(res.get, gtruth.get, 0.8, Some(20)))
          lb += ("resultquality-rbo0.98@20" -> getRBO(res.get, gtruth.get, 0.98, Some(20)))

          lb += ("resultquality-rbo0.05@50" -> getRBO(res.get, gtruth.get, 0.05, Some(50)))
          lb += ("resultquality-rbo0.1@50" -> getRBO(res.get, gtruth.get, 0.1, Some(50)))
          lb += ("resultquality-rbo0.2@50" -> getRBO(res.get, gtruth.get, 0.2, Some(50)))
          lb += ("resultquality-rbo0.5@50" -> getRBO(res.get, gtruth.get, 0.5, Some(50)))
          lb += ("resultquality-rbo0.8@50" -> getRBO(res.get, gtruth.get, 0.8, Some(50)))
          lb += ("resultquality-rbo0.98@50" -> getRBO(res.get, gtruth.get, 0.98, Some(50)))

          lb += ("resultquality-rbo0.05@100" -> getRBO(res.get, gtruth.get, 0.05, Some(100)))
          lb += ("resultquality-rbo0.1@100" -> getRBO(res.get, gtruth.get, 0.1, Some(100)))
          lb += ("resultquality-rbo0.2@100" -> getRBO(res.get, gtruth.get, 0.2, Some(100)))
          lb += ("resultquality-rbo0.5@100" -> getRBO(res.get, gtruth.get, 0.5, Some(100)))
          lb += ("resultquality-rbo0.8@100" -> getRBO(res.get, gtruth.get, 0.8, Some(100)))
          lb += ("resultquality-rbo0.98@100" -> getRBO(res.get, gtruth.get, 0.98, Some(100)))

        } else {
          lb += ("resultquality" -> gtruth.failed.get.getMessage)
        }
      }
    } else {
      lb += ("failure" -> res.failed.get.getMessage)
    }

    lb.toMap.mapValues(_.toString)
  }


  private def getResults(res: Seq[RPCQueryResults], k: Option[Int]): Seq[String] = {
    val resPKs = res.map(_.results.sortBy(r => r.getOrElse("ap_distance", "-1.0").toDouble).map(_.get("ap_id"))).head.map(_.get)

    if (k.isDefined) {
      resPKs.take(k.get)
    } else {
      resPKs
    }
  }

  /**
    *
    * @param res
    * @param truth
    * @return
    */
  protected def getSimpleRecall(res: Seq[RPCQueryResults], truth: Seq[RPCQueryResults], k: Option[Int]): Double = {
    val truthPKs = getResults(truth, None)
    val resPKs = getResults(res, None)

    val agreements = truthPKs.intersect(resPKs).length

    val measure = (agreements / k.getOrElse(truthPKs.length).toDouble)
    measure
  }

  /**
    *
    * @param res
    * @param truth
    * @param k
    * @return
    */
  protected def getCompetitiveRecallAtK(res: Seq[RPCQueryResults], truth: Seq[RPCQueryResults], k: Option[Int]): Double = {
    val truthPKs = getResults(truth, k)
    val resPKs = getResults(res, k)

    val agreements = truthPKs.intersect(resPKs).length
    //simple hits/total
    val measure = (agreements / k.getOrElse(truthPKs.length).toDouble)
    measure
  }


  /**
    *
    * @param res
    * @param truth
    * @param p
    * @param k
    * @return
    */
  protected def getRBO(res: Seq[RPCQueryResults], truth: Seq[RPCQueryResults], p: Double, k: Option[Int]): Double = {
    val truthPKs = getResults(truth, k)
    val resPKs = getResults(res, k)

    val s = math.min(resPKs.length, truthPKs.length)
    val l = math.max(resPKs.length, truthPKs.length)

    val intersects = (1 to math.max(resPKs.length, truthPKs.length)).map(i => (truthPKs.take(i) intersect resPKs.take(i)).length).map(_.toDouble)

    val sum1 = intersects.zip(Stream.from(1).map(_.toDouble)).map { case (intersect, i) => (math.pow(p, i) / (i)) * intersect }.sum

    val sum2 = ((s + 1) to l).map(i => intersects(i - 1) * (i - s) / (i * s).toDouble * math.pow(p, i)).sum

    val sum3 = ((intersects(l - 1) - intersects(s - 1)) / l.toDouble + (intersects(s - 1) / s.toDouble)) * math.pow(p, l)

    val measure = (1 - p) / p * (sum1 + sum2) + sum3
    measure
  }


  /**
    *
    * @param res
    * @param truth
    * @param k
    * @return
    */
  protected def getAverageOverlap(res: Seq[RPCQueryResults], truth: Seq[RPCQueryResults], k: Option[Int]): Double = {
    val truthPKs = getResults(truth, k)
    val resPKs = getResults(res, k)

    val s = math.min(resPKs.length, truthPKs.length)
    val l = math.max(resPKs.length, truthPKs.length)

    val intersects = (1 to math.max(resPKs.length, truthPKs.length)).map(i => (truthPKs.take(i) intersect resPKs.take(i)).length).map(_.toDouble)

    val x = (1 to l).map(i => 2 * (intersects(i - 1) / (i.toDouble + math.min(i, s))))
    val measure = (1 / l.toDouble) * x.sum
    measure
  }


  /**
    *
    * @param results
    * @return
    */
  protected def prepareResults(results: ListBuffer[(String, Map[String, String])]) = {
    //fill properties
    val prop = new Properties
    prop.setProperty("evaluation_mode", job.general_mode)

    results.foreach { case (runid, result) =>
      result.map { case (k, v) => (runid + "_" + k) -> v } //remap key
        .foreach { case (k, v) => prop.setProperty(k, v) } //set property
    }

    //get overview for plotting
    val times = results.map { case (runid, result) => result.get("totaltime").getOrElse("-1") }
    val quality = results.map { case (runid, result) => result.get("resultquality").getOrElse("-1") }

    prop.setProperty("summary_data_vector_dimensions", job.data_vector_dimensions.toString)
    prop.setProperty("summary_data_tuples", job.data_tuples.toString)

    prop.setProperty("summary_execution_name", job.execution_name)
    prop.setProperty("summary_execution_subtype", job.execution_subtype)

    prop.setProperty("summary_totaltime", times.mkString(","))
    prop.setProperty("summary_resultquality", quality.mkString(","))


    //available metrics
    val metrics = results.map { case (runid, result) => result.keySet.filter(_.startsWith("resultquality-")) }.flatten.toSet

    val summaryMetrics = ListBuffer[String]()

    metrics.foreach { metric =>
      val summaryName = "summary_" + metric
      val quality = results.map { case (runid, result) => result.get(metric).getOrElse("-1") }

      prop.setProperty(summaryName, quality.mkString(","))

      summaryMetrics += summaryName
    }

    prop.setProperty("summary_resultquality_metrics", summaryMetrics.mkString(","))


    prop
  }


  /**
    * Aborts the further running of queries.
    */
  def abort() {
    running = false
  }


  /**
    *
    * @param that
    * @return
    */
  override def equals(that: Any): Boolean =
    that match {
      case that: Executor =>
        this.job.id == that.job.id
      case _ => false
    }

  /**
    *
    * @return
    */
  override def hashCode: Int = job.id
}
