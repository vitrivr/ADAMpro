package ch.unibas.dmi.dbis.adam.chronos

import java.io.File
import java.util.Properties
import java.util.logging.{Level, LogRecord}

import ch.unibas.cs.dbis.chronos.agent.{ChronosHttpClient, ChronosJob}
import ch.unibas.dmi.dbis.adam.rpc.RPCClient
import ch.unibas.dmi.dbis.adam.rpc.datastructures.{RPCAttributeDefinition, RPCQueryObject, RPCQueryResults}

import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class EvaluationExecutor(val job: EvaluationJob, logger: ChronosHttpClient#ChronosLogHandler, setStatus : (Double) => (Boolean), inputDirectory: File, outputDirectory: File) {
  //rpc client
  val client: RPCClient = RPCClient(job.adampro_url, job.adampro_port)

  //if job has been aborted, running will be set to false so that no new queries are started
  var running = true
  var progress = 0.0

  /**
    *
    * @param job
    * @param logger
    * @param setStatus
    * @param inputDirectory
    * @param outputDirectory
    */
  def this(job: ChronosJob, logger: ChronosHttpClient#ChronosLogHandler, setStatus : (Double) => (Boolean), inputDirectory: File, outputDirectory: File) {
    this(new EvaluationJob(job), logger, setStatus, inputDirectory, outputDirectory)
  }


  /**
    * Runs evaluation.
    */
  def run(): Properties = {
    val results = new ListBuffer[(String, Map[String, String])]()
    val entityname = generateString(10)
    val attributes = getAttributeDefinition()

    //create entity
    logger.publish(new LogRecord(Level.INFO, "creating entity " + entityname + " (" + attributes.map(a => a.name + "(" + a.datatype + ")").mkString(",") + ")"))
    client.entityCreate(entityname, attributes)

    //insert random data
    logger.publish(new LogRecord(Level.INFO, "inserting " + job.data_tuples + " tuples into " + entityname))
    client.entityGenerateRandomData(entityname, job.data_tuples, job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max, job.data_vector_sparse)

    val indexnames = if (job.execution_name == "sequential") {
      //no index
      logger.publish(new LogRecord(Level.INFO, "creating no index for " + entityname))
      Seq()
    } else if (job.execution_name == "progressive") {
      logger.publish(new LogRecord(Level.INFO, "creating all indexes for " + entityname))
      client.entityCreateAllIndexes(entityname, Seq("fv0"), 2).get
    } else {
      logger.publish(new LogRecord(Level.INFO, "creating " + job.execution_name + " index for " + entityname))
      Seq(client.indexCreate(entityname, "fv0", job.execution_name, 2, Map()).get)
    }

    //partition
    getPartitionCombinations().foreach { case (e, i) =>
      if (e.isDefined) {
        client.entityPartition(entityname, e.get, Seq(), true, true)
      }

      if (i.isDefined) {
        indexnames.foreach(indexname => client.indexPartition(indexname, i.get, Seq(), true, true))
      }

      //collect queries
      logger.publish(new LogRecord(Level.INFO, "generating queries to execute on " + entityname))
      val queries = getQueries()

      //query execution
      queries.zipWithIndex.foreach { case (qo, idx) =>
        if (running) {
          val runid = generateString(10)
          logger.publish(new LogRecord(Level.INFO, "executing query for " + entityname + " (runid: " + runid + ")"))
          val result = executeQuery(qo)
          logger.publish(new LogRecord(Level.INFO, "executed query for " + entityname + " (runid: " + runid + ")"))
          results += (runid -> result)
        } else {
          logger.publish(new LogRecord(Level.INFO, "aborted job " + job.id + ", not running queries anymore"))
        }

        progress += 1 / queries.size.toFloat
      }
    }

    logger.publish(new LogRecord(Level.INFO, "all queries for job " + job.id + " have been run, preparing data and finishing execution"))

    //fill properties
    val prop = new Properties
    results.foreach { case (runid, result) =>
      result.map { case (k, v) => (runid + "_" + k) -> v } //remap key
        .foreach { case (k, v) => prop.setProperty(k, v) } //set property
    }

    prop
  }

  /**
    * Aborts the further running of queries.
    */
  def abort() {
    running = false
  }

  /**
    * Returns progress (0 - 1)
    *
    * @return
    */
  def getProgress: Double = progress

  /**
    * Gets a schema for an entity to create.
    *
    * @return
    */
  private def getAttributeDefinition(): Seq[RPCAttributeDefinition] = {
    val lb = new ListBuffer[RPCAttributeDefinition]()

    //pk
    lb.append(RPCAttributeDefinition("pk", job.data_vector_pk, true, true, true))

    //vector
    lb.append(RPCAttributeDefinition("fv0", "feature"))

    //metadata
    val metadata = Map("long" -> job.data_metadata_long, "int" -> job.data_metadata_int,
      "float" -> job.data_metadata_float, "double" -> job.data_metadata_double,
      "string" -> job.data_metadata_string, "text" -> job.data_metadata_text,
      "boolean" -> job.data_metadata_boolean
    )

    metadata.foreach { case (datatype, number) =>
      (0 until number).foreach { i =>
        lb.append(RPCAttributeDefinition(datatype + "i", datatype))
      }
    }

    lb.toSeq
  }

  /**
    * Returns combinations of partitionings.
    *
    * @return
    */
  private def getPartitionCombinations(): Seq[(Option[Int], Option[Int])] = {
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
  private def getQueries(): Seq[RPCQueryObject] = {
    val lb = new ListBuffer[RPCQueryObject]()

    job.query_k.flatMap { k =>
      val denseQueries = (0 to job.query_dense_n).map { i => getQuery(k, false) }
      val sparseQueries = (0 to job.query_sparse_n).map { i => getQuery(k, true) }

      denseQueries union sparseQueries
    }
  }

  /**
    * Gets single query.
    *
    * @param k
    * @param sparseQuery
    * @return
    */
  def getQuery(k: Int, sparseQuery: Boolean): RPCQueryObject = {
    val lb = new ListBuffer[(String, String)]()

    lb.append("k" -> k.toString)


    lb.append("distance" -> job.query_distance)

    if (job.query_denseweighted || job.query_sparseweighted) {
      lb.append("weights" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))
    }

    if (job.query_sparseweighted) {
      lb.append("sparseweights" -> "true")
    }


    lb.append("query" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))

    if (sparseQuery) {
      lb.append("sparsequery" -> "true")
    }

    if (job.execution_withsequential) {
      lb.append("indexonly" -> "false")
    }

    lb.append("informationlevel" -> "final_only")

    lb.append("hints" -> job.execution_hint)

    RPCQueryObject(generateString(10), job.execution_name, lb.toMap, None)
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
  private def generateFeatureVector(dimensions: Int, sparsity: Float, min: Float, max: Float) = {
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
  private def sparsify(vec: Seq[Float]) = {
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
    * Generates a string (only a-z).
    *
    * @param nletters
    * @return
    */
  private def generateString(nletters: Int) = (0 until nletters).map(x => Random.nextInt(26)).map(x => ('a' + x).toChar).mkString


  /**
    * Executes a query.
    *
    * @param qo
    */
  private def executeQuery(qo: RPCQueryObject): Map[String, String] = {
    val lb = new ListBuffer[(String, Any)]()

    lb ++= (job.getAllParameters())

    logger.publish(new LogRecord(Level.FINEST, "executing query with parameters: " + job.getAllParameters().mkString))

    lb += ("queryid" -> qo.id)
    lb += ("operation" -> qo.operation)
    lb += ("options" -> qo.options.mkString)
    lb += ("debugQuery" -> qo.getQueryMessage.toString())

    if (job.execution_name != "progressive") {
      val t1 = System.currentTimeMillis

      //do query
      val res: Try[Seq[RPCQueryResults]] = client.doQuery(qo)


      val t2 = System.currentTimeMillis

      lb += ("starttime" -> t1)
      lb += ("isSuccess" -> res.isSuccess)
      lb += ("nResults" -> res.map(_.length).getOrElse(0))
      lb += ("endtime" -> t2)

      if (res.isSuccess) {
        lb += ("measuredtime" -> res.get.map(_.time).mkString(";"))
        lb += ("results" -> res.get.map(r => (r.results).mkString).mkString(";;"))
      } else {
        lb += ("failure" -> res.failed.get.getMessage)
      }
    } else {
      var isCompleted = false
      val t1 = System.currentTimeMillis
      var t2 = System.currentTimeMillis - 1 //returning -1 on error

      //do progressive query
      client.doProgressiveQuery(qo,
        next = (res) => ({
          if (res.isSuccess) {
            val confidence = res.get._2
            val source = res.get._3
            val time = res.get._4
            val results = res.get._5

            lb += (source + "confidence" -> confidence)
            lb += (source + "source" -> source)
            lb += (source + "time" -> time)
            lb += (source + "results" -> results.mkString)
          } else {
            lb += ("failure" -> res.failed.get.getMessage)
          }
        }),
        completed = (id) => ({
          isCompleted = true
          t2 = System.currentTimeMillis
        }))

      while (!isCompleted) {
        Thread.sleep(1000)
      }

      lb += ("starttime" -> t1)
      lb += ("endtime" -> t2)
    }

    lb.toMap.mapValues(_.toString)
  }


  override def equals(that: Any): Boolean =
    that match {
      case that: EvaluationExecutor =>
        this.job.id == that.job.id
      case _ => false
    }

  override def hashCode: Int = job.id
}
