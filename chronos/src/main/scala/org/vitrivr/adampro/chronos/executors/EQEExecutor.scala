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
class EQEExecutor(job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) extends Executor(job, setStatus, inputDirectory, outputDirectory) {
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

    logger.info("started adjusting query weights")

    val nqueries = if(job.execution_nqueries.isEmpty){
      None
    } else {
      Some(job.execution_nqueries.toInt)
    }

    val nruns = if(job.execution_nruns.isEmpty){
      None
    } else {
      Some(job.execution_nruns.toInt)
    }

    client.entityAdaptScanMethods(entityname.get, job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME), Some(job.execution_subtype), false, false, nqueries = nqueries, nruns = nruns)
    logger.info("adjusted query weights")

    updateStatus(0.5)

    //collect queries
    logger.info("generating queries to execute on " + entityname.get)
    val queries = getQueries(entityname.get)

    val queryProgressAddition = (1 - getStatus) / queries.size.toFloat

    //query execution
    queries.zipWithIndex.foreach { case (qo, idx) =>
      if (running) {
        val runid = "run_" + idx.toString
        logger.info("executing query for " + entityname.get + " (runid: " + runid + ")")

        val scoring = client.getScoredQueryExecutionPaths(qo, job.execution_subtype).get.map(x => (x._1, x._2, x._3))
          .groupBy(_._2).mapValues(_.sortBy(_._3).reverse.head).toMap
        //index name, type, score; take only maximum

        val result = executeQuery(qo)

        //execute all other scans for comparison
        job.execution_subexecution.map { case (indextype, withsequential) => (indextype, scoring.get(indextype)) }
          .filter(_._2.isDefined)
          .filterNot(x => x._1 == "sequential")
          .foreach { case (indextype, scoring) =>
            val tmpQo = new RPCComplexQueryObject(qo.id, qo.options ++ Seq("indexname" -> scoring.get._1), "index", qo.targets)
            val tmpResult = executeQuery(tmpQo) ++ Seq("scanscore" -> scoring.get._3.toString)
            results += (runid + "-loosers-" + indextype -> tmpResult)
          }

        if(job.execution_subexecution.map(_._1).contains("sequential")){
          val tmpQo = new RPCComplexQueryObject(qo.id, qo.options, "sequential", qo.targets)
          val tmpResult = executeQuery(tmpQo) ++ Seq("scanscore" -> scoring.get("sequential").map(_._3).getOrElse(-1).toString)
          results += (runid + "-loosers-" + "sequential" -> tmpResult)
        }

        logger.info("executed query for " + entityname.get + " (runid: " + runid + ")")

        if (job.measurement_firstrun && idx == 0) {
          //ignore first run
        } else {
          results += (runid + "-winner" -> result)
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
    * Gets single query.
    *
    * @param k
    * @param sparseQuery
    * @return
    */
  override protected def getQuery(entityname: String, k: Int, sparseQuery: Boolean, options : Seq[(String, String)] = Seq()): RPCComplexQueryObject = {
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

    //confidence level
    lb.append("confidence" -> job.execution_confidence)

    lb.append("hints" -> job.execution_hint)

    RPCComplexQueryObject(Helpers.generateString(10), (options ++ lb).toMap, job.execution_name, None)
  }
}
