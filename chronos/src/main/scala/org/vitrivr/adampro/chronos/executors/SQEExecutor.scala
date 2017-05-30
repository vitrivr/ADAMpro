package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.{CreationHelper, Helpers}
import org.vitrivr.adampro.rpc.datastructures.{RPCComplexQueryObject, RPCGenericQueryObject, RPCStochasticScanQueryObject}

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
class SQEExecutor(job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) extends Executor(job, setStatus, inputDirectory, outputDirectory) {
  /**
    * Runs evaluation.
    */
  def run(): Properties = {
    val results = new ListBuffer[(String, Map[String, String])]()

    updateStatus(0)

    val entityname = CreationHelper.createEntity(client, job)
    assert(entityname.isSuccess)

    val indexnames = CreationHelper.createIndexes(client, job, entityname.get)
    assert(indexnames.isSuccess)

    updateStatus(0.25)


    //collect queries
    logger.info("generating queries to execute on " + indexnames.get.mkString(", "))
    val queries = getSQEQueries(entityname.get, indexnames.get)

    val queryProgressAddition = (1 - getStatus) / queries.size.toFloat

    //query execution
    queries.zipWithIndex.foreach { case (qo, idx) =>
      if (running) {
        val runid = "run_" + idx.toString
        logger.info("executing query for " + entityname.get + " (runid: " + runid + ")")
        var result = executeQuery(qo)
        logger.info("executed query for " + entityname.get + " (runid: " + runid + ")")

        if (job.measurement_firstrun && idx < NFIRST_RUN_QUERIES) {
          //ignore first run
        } else {
          results += (runid -> result)
        }

      } else {
        logger.warning("aborted job " + job.id + ", not running queries anymore")
      }

      updateStatus(getStatus + queryProgressAddition)
    }

    logger.info("all queries for job " + job.id + " have been run, preparing data and finishing execution")

    val prop = prepareResults(results)


    //clean up
    if (job.maintenance_delete) {
      client.entityDrop(entityname.get)
    }

    prop
  }

  /**
    * Gets queries.
    *
    * @return
    */
  protected def getSQEQueries(entityname : String, indexnames: Seq[String], options : Seq[(String, String)] = Seq()): Seq[RPCGenericQueryObject] = {
    val lb = new ListBuffer[RPCGenericQueryObject]()

    val additionals = if (job.measurement_firstrun) {
      NFIRST_RUN_QUERIES
    } else {
      0
    }

    job.query_k.flatMap { k =>
      val denseQueries = (0 until (job.query_n + additionals)).map { i => getSQEQuery(entityname, indexnames, k, false, options) }

      denseQueries
    }
  }


  /**
    * Gets single query
    *
    * @param entityname
    * @param indexnames
    * @param k
    * @param sparseQuery
    * @param options
    * @return
    */
  protected def getSQEQuery(entityname : String, indexnames: Seq[String], k: Int, sparseQuery: Boolean, options : Seq[(String, String)] = Seq()): RPCGenericQueryObject = {
    val id = Helpers.generateString(10)

    val lb = new ListBuffer[(String, String)]()

    lb.append("entityname" -> entityname)

    lb.append("indexnames" -> indexnames.mkString(","))

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

    lb.append("informationlevel" -> "final_only")

    lb.append("hints" -> job.execution_hint)

    if (job.execution_name == "index") {
      lb.append("subtype" -> job.execution_subtype)
    }

    RPCStochasticScanQueryObject(id, (options ++ lb).toMap)
  }
}
