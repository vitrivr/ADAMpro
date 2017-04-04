package org.vitrivr.adampro.chronos

import java.io.File
import java.util.Properties
import java.util.logging.Logger

import ch.unibas.dmi.dbis.chronos.agent.ChronosJob
import org.vitrivr.adampro.grpc.grpc.RepartitionMessage
import org.vitrivr.adampro.rpc.RPCClient
import org.vitrivr.adampro.rpc.datastructures.{RPCAttributeDefinition, RPCQueryResults, RPCQueryObject}

import scala.collection.mutable.ListBuffer
import scala.util.{Random, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class EvaluationExecutor(val job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) {
  val logger: Logger = Logger.getLogger("ADAMpro")

  //rpc client
  val client: RPCClient = RPCClient(job.adampro_url, job.adampro_port)

  //if job has been aborted, running will be set to false so that no new queries are started
  var running = true
  var progress = 0.0

  /**
    *
    * @param job
    * @param setStatus
    * @param inputDirectory
    * @param outputDirectory
    */
  def this(job: ChronosJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) {
    this(new EvaluationJob(job), setStatus, inputDirectory, outputDirectory)
  }

  private val ENTITY_NAME_PREFIX = "chron_eval_"
  private val FEATURE_VECTOR_ATTRIBUTENAME = "vector"

  /**
    * Runs evaluation.
    */
  def run(): Properties = {
    val results = new ListBuffer[(String, Map[String, String])]()

    val entityname = if(job.data_enforcecreation) {
      //generate a new entity with a random name
      generateString(10)
    } else {
      //get entity based on creation attributes
      getEntityName()
    }
    val attributes = getAttributeDefinition()

    var entityCreatedNewly = false

    //create entity
    if(client.entityExists(entityname).get){
      logger.info("entity " + entityname + " exists already")
      entityCreatedNewly = false
    } else {
      logger.info("creating entity " + entityname + " (" + attributes.map(a => a.name + "(" + a.datatype + ")").mkString(",") + ")")

      val entityCreatedRes = client.entityCreate(entityname, attributes)

      if(entityCreatedRes.isFailure){
        logger.severe(entityCreatedRes.failed.get.getMessage)
        throw entityCreatedRes.failed.get
      }

      entityCreatedNewly = true
    }

    //insert random data
    logger.info("inserting " + job.data_tuples + " tuples into " + entityname)
    client.entityGenerateRandomData(entityname, job.data_tuples, job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max, job.data_vector_sparse)

    var indexCreatedNewly = false

    val indexnames = if (job.execution_name == "sequential") {
      //no index
      logger.info("creating no index for " + entityname)
      Seq()
    } else if (job.execution_name == "progressive") {
      logger.info("creating all indexes for " + entityname)
      indexCreatedNewly = true
      client.entityCreateAllIndexes(entityname, Seq(FEATURE_VECTOR_ATTRIBUTENAME), 2).get
    } else {
      if(client.indexExists(entityname, FEATURE_VECTOR_ATTRIBUTENAME, job.execution_subtype).get) {
        logger.info(job.execution_subtype + " index for " + entityname + " (" +  FEATURE_VECTOR_ATTRIBUTENAME + ") " + "exists already")
        indexCreatedNewly = false
        client.indexList(entityname).get.filter(_._2 == FEATURE_VECTOR_ATTRIBUTENAME).filter(_._3 == job.execution_subtype).map(_._1)
      } else {
        logger.info("creating " + job.execution_subtype + " index for " + entityname)
        indexCreatedNewly = true
        Seq(client.indexCreate(entityname, FEATURE_VECTOR_ATTRIBUTENAME, job.execution_subtype, 2, Map()).get)
      }
    }

    if (job.measurement_cache) {
      indexnames.foreach { indexname =>
        client.indexCache(indexname)
      }

      client.entityCache(entityname)
    }

    //partition
    getPartitionCombinations().foreach { case (e, i) =>
      if (e.isDefined) {
        if(RepartitionMessage.Partitioner.values.find(p => p.name == job.access_entity_partitioner).isDefined){
          client.entityPartition(entityname, e.get, None, true, true, job.access_index_partitioner)
        } else client.entityPartition(entityname, e.get, None, true, true)
        //TODO: add partition column to job
      }

      if (i.isDefined) {
        //TODO: add partition column to job
        if(RepartitionMessage.Partitioner.values.find(p => p.name == job.access_index_partitioner).isDefined){
          indexnames.foreach(indexname => client.indexPartition(indexname, i.get, None, true, true, job.access_index_partitioner))
        } else indexnames.foreach(indexname => client.indexPartition(indexname, i.get, None, true, true))
      }

      //collect queries
      logger.info("generating queries to execute on " + entityname)
      val queries = getQueries(entityname)

      //query execution
      queries.zipWithIndex.foreach { case (qo, idx) =>
        if (running) {
          val runid = "r-" + idx.toString
          logger.info("executing query for " + entityname + " (runid: " + runid + ")")
          var result = executeQuery(qo)

          //further params to log
          result += "entityCreatedNewly" -> entityCreatedNewly.toString
          result += "indexCreatedNewly" -> indexCreatedNewly.toString

          logger.info("executed query for " + entityname + " (runid: " + runid + ")")

          if (job.measurement_firstrun && idx == 0){
            //ignore first run
          } else {
            results += (runid -> result)
          }

        } else {
          logger.info("aborted job " + job.id + ", not running queries anymore")
        }

        progress += 1 / queries.size.toFloat
      }
    }

    logger.info("all queries for job " + job.id + " have been run, preparing data and finishing execution")

    //fill properties
    val prop = new Properties
    results.foreach { case (runid, result) =>
      result.map { case (k, v) => (runid + "_" + k) -> v } //remap key
        .foreach { case (k, v) => prop.setProperty(k, v) } //set property
    }

    if(job.data_enforcecreation) {
      client.entityDrop(entityname)
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
    * Generates an entity name based on the parameters chosen for the entity.
    * @return
    */
  private def getEntityName() : String = {
    val prime = 31
    var result = 1
    result = prime * result + job.data_tuples.hashCode
    result = prime * result + job.data_vector_dimensions.hashCode
    result = prime * result + job.data_vector_min.hashCode
    result = prime * result + job.data_vector_max.hashCode
    result = prime * result + job.data_vector_sparse.hashCode
    result = prime * result + job.data_vector_sparsity.hashCode
    result = prime * result + job.data_metadata_boolean.hashCode
    result = prime * result + job.data_metadata_double.hashCode
    result = prime * result + job.data_metadata_float.hashCode
    result = prime * result + job.data_metadata_int.hashCode
    result = prime * result + job.data_metadata_string.hashCode
    result = prime * result + job.data_metadata_long.hashCode
    result = prime * result + job.data_metadata_text.hashCode
    result = prime * result + job.data_vector_pk.hashCode

    ENTITY_NAME_PREFIX + result.toString.replace("-","m")
  }

  /**
    * Gets a schema for an entity to create.
    *
    * @return
    */
  private def getAttributeDefinition(): Seq[RPCAttributeDefinition] = {
    val lb = new ListBuffer[RPCAttributeDefinition]()

    //pk
    lb.append(RPCAttributeDefinition("id", job.data_vector_pk, params = Map("indexed" -> "true")))

    //vector
    lb.append(RPCAttributeDefinition(FEATURE_VECTOR_ATTRIBUTENAME, "vector"))

    //metadata
    val metadata = Map("long" -> job.data_metadata_long, "int" -> job.data_metadata_int,
      "float" -> job.data_metadata_float, "double" -> job.data_metadata_double,
      "string" -> job.data_metadata_string, "text" -> job.data_metadata_text,
      "boolean" -> job.data_metadata_boolean
    )

    metadata.foreach { case (datatype, number) =>
      (0 until number).foreach { i =>
        lb.append(RPCAttributeDefinition(datatype + "i", datatype, Some("parquet")))
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
  private def getQueries(entityname : String): Seq[RPCQueryObject] = {
    val lb = new ListBuffer[RPCQueryObject]()

    val additionals = if (job.measurement_firstrun) { 1 } else { 0 }

    job.query_k.flatMap { k =>
      val denseQueries = (0 to job.query_dense_n + additionals).map { i => getQuery(entityname, k, false) }
      val sparseQueries = (0 to job.query_sparse_n + additionals).map { i => getQuery(entityname, k, true) }

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
  def getQuery(entityname : String, k: Int, sparseQuery: Boolean): RPCQueryObject = {
    val lb = new ListBuffer[(String, String)]()

    lb.append("entityname" -> entityname)

    lb.append("attribute" -> FEATURE_VECTOR_ATTRIBUTENAME)

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

    if(job.execution_name == "index"){
      lb.append("subtype" -> job.execution_subtype)
    }

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

    logger.fine("executing query with parameters: " + job.getAllParameters().mkString)

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
        //time
        lb += ("measuredtime" -> res.get.map(_.time).mkString(";"))

        //results
        lb += ("results" -> {
          res.get.head.results.map(res => (res.get("pk") + "," + res.get("adamprodistance"))).mkString("(", "),(", ")")
        })

        //result quality
        if(job.measurement_resultquality){
          //perform sequential query
          val opt = collection.mutable.Map() ++ qo.options
          opt -= "hints"
          opt += "hints" -> "sequential"
          val gtruth = client.doQuery(qo.copy(options = opt.toMap))

          if(gtruth.isSuccess){
            val gtruthPKs = gtruth.get.map(_.results.map(_.get("pk")))
            val resPKs = res.get.map(_.results.map(_.get("pk")))

            val agreements = gtruthPKs.intersect(resPKs).length
            //simple hits/total
            lb+= ("resultquality" -> agreements / qo.options.get("k").get.toInt)
          } else{
            lb+= ("resultquality" -> gtruth.failed.get.getMessage)
          }
        }
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
            lb += (res.get.source + "confidence" -> res.get.confidence)
            lb += (res.get.source + "source" -> res.get.source)
            lb += (res.get.source + "time" -> res.get.time)
            lb += (res.get.source + "results" -> {
              res.get.results.map(res => (res.get("pk") + "," + res.get("adamprodistance"))).mkString("(", "),(", ")")
            })
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
