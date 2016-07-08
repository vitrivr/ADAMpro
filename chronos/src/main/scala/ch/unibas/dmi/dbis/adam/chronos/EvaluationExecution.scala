package ch.unibas.dmi.dbis.adam.chronos

import ch.unibas.dmi.dbis.adam.rpc.RPCClient
import ch.unibas.dmi.dbis.adam.rpc.datastructures.{RPCQueryObject, RPCAttributeDefinition}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class EvaluationExecution(job: EvaluationJob, client: RPCClient) {

  def run(): Unit = {
    val entityname = generateString(10)
    val attributes = getAttributeDefinition()

    //create entity
    client.entityCreate(entityname, attributes)

    //insert random data
    client.entityGenerateRandomData(entityname, job.data_tuples, job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max, job.data_vector_sparse)

    val indexnames = if (job.execution_name == "sequential") {
      //no index
      Seq()
    } else if (job.execution_name == "progressive") {
      client.entityCreateAllIndexes(entityname, Seq("fv0"), 2).get
    } else {
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
      val queries = getQueries()

      //query execution
      queries.foreach { qo =>
        executeQuery(qo)
      }
    }
  }


  /**
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
    *
    * @param nletters
    * @return
    */
  private def generateString(nletters: Int) = (0 until nletters).map(x => Random.nextInt(26)).map(x => ('a' + x).toChar).mkString


  /**
    *
    * @param qo
    */
  private def executeQuery(qo: RPCQueryObject): Unit = {
    if (job.execution_name == "progressive") {
      client.doQuery(qo)
      //TODO: add logging, storing results
      //log
    } else {
      client.doProgressiveQuery(qo, (res) => (), (comp) => ())
      //TODO: add logging, storing results
      //log
    }
  }

}
