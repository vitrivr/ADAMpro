package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.{CreationHelper, Helpers}
import org.vitrivr.adampro.rpc.datastructures.RPCComplexQueryObject

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
    val queries = getSQEQueries(indexnames.get)

    val queryProgressAddition = (1 - getStatus) / queries.size.toFloat

    //query execution
    queries.zipWithIndex.foreach { case (qo, idx) =>
      if (running) {
        val runid = "run_" + idx.toString
        logger.info("executing query for " + entityname.get + " (runid: " + runid + ")")
        var result = executeQuery(qo)
        logger.info("executed query for " + entityname.get + " (runid: " + runid + ")")

        if (job.measurement_firstrun && idx == 0) {
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
  protected def getSQEQueries(indexes: Seq[String], options : Seq[(String, String)] = Seq()): Seq[RPCComplexQueryObject] = {
    val lb = new ListBuffer[RPCComplexQueryObject]()

    val additionals = if (job.measurement_firstrun) {
      1
    } else {
      0
    }

    job.query_k.flatMap { k =>
      val denseQueries = (0 to job.query_n + additionals).map { i => getSQEQuery(indexes, k, false, options) }

      denseQueries
    }
  }


  /**
    * Gets single query
    *
    * @param indexes
    * @param k
    * @param sparseQuery
    * @param options
    * @return
    */
  protected def getSQEQuery(indexes: Seq[String], k: Int, sparseQuery: Boolean, options : Seq[(String, String)] = Seq()): RPCComplexQueryObject = {
    val lb = new ListBuffer[(String, String)]()

    lb.append("indexes" -> indexes.mkString(","))

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

    RPCComplexQueryObject(Helpers.generateString(10), (options ++ lb).toMap, job.execution_name, None)
  }
}
