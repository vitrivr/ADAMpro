package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties
import java.util.logging.Logger

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.Helpers
import org.vitrivr.adampro.rpc.RPCClient
import org.vitrivr.adampro.rpc.datastructures.{RPCQueryObject, RPCQueryResults}

import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
abstract class Executor(val job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) {
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
  protected def getQueries(entityname: String): Seq[RPCQueryObject] = {
    val lb = new ListBuffer[RPCQueryObject]()

    val additionals = if (job.measurement_firstrun) {
      1
    } else {
      0
    }

    job.query_k.flatMap { k =>
      val denseQueries = (0 to job.query_n + additionals).map { i => getQuery(entityname, k, false) }

      denseQueries
    }
  }


  /**
    * Gets single query.
    *
    * @param k
    * @param sparseQuery
    * @return
    */
  protected def getQuery(entityname: String, k: Int, sparseQuery: Boolean): RPCQueryObject = {
    val lb = new ListBuffer[(String, String)]()

    lb.append("entityname" -> entityname)

    lb.append("attribute" -> job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME))

    lb.append("k" -> k.toString)

    lb.append("distance" -> job.query_distance)

    if (job.query_weighted) {
      lb.append("weights" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))
    }

    lb.append("query" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))

    if (sparseQuery) {
      lb.append("sparsequery" -> "true")
    }

    if (job.execution_withsequential) {
      lb.append("indexonly" -> "false")
    }

    lb.append("informationlevel" -> "minimal")

    lb.append("hints" -> job.execution_hint)

    if(job.execution_subtype != null && job.execution_subtype.length > 0){
      lb.append("subtype" -> job.execution_subtype)
    }

    RPCQueryObject(Helpers.generateString(10), job.execution_name, lb.toMap, None)
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
  protected def generateFeatureVector(dimensions: Int, sparsity: Float, min: Float, max: Float) = {
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
  protected def executeQuery(qo: RPCQueryObject): Map[String, String] = {
    val lb = new ListBuffer[(String, Any)]()

    lb ++= (job.getAllParameters())

    logger.fine("executing query with parameters: " + job.getAllParameters().mkString)

    lb += ("queryid" -> qo.id)
    lb += ("operation" -> qo.operation)
    lb += ("options" -> qo.options.mkString)
    lb += ("debugQuery" -> qo.getQueryMessage.toString())

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
        res.get.head.results.map(res => (res.get("ap_id") + "," + res.get("ap_distance"))).mkString("(", "),(", ")")
      })

      //result quality
      if (job.measurement_resultquality) {
        //perform sequential query
        val opt = collection.mutable.Map() ++ qo.options
        opt -= "hints"
        opt += "hints" -> "sequential"
        val gtruth = client.doQuery(qo.copy(options = opt.toMap))

        if (gtruth.isSuccess) {
          val gtruthPKs = gtruth.get.map(_.results.map(_.get("ap_id"))).head.map(_.get)
          val resPKs = res.get.map(_.results.map(_.get("ap_id"))).head.map(_.get)

          val agreements = gtruthPKs.intersect(resPKs).length
          //simple hits/total
          val quality = (agreements / qo.options.get("k").get.toDouble)
          lb += ("resultquality" -> quality.toString)
        } else {
          lb += ("resultquality" -> gtruth.failed.get.getMessage)
        }
      }
    } else {
      lb += ("failure" -> res.failed.get.getMessage)
    }

    lb.toMap.mapValues(_.toString)
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
