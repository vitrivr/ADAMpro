package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.CreationHelper
import org.vitrivr.adampro.rpc.datastructures.RPCQueryObject

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

    client.entityTrainScanWeights(entityname.get, job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME), false, false)

    updateStatus(0.25)

    //collect queries
    logger.info("generating queries to execute on " + entityname.get)
    val queries = getQueries(entityname.get)

    val queryProgressAddition = (1 - getStatus) / queries.size.toFloat

    //query execution
    queries.zipWithIndex.foreach { case (qo, idx) =>
      if (running) {

        val scoring = client.getScoredQueryExecutionPaths(qo, job.execution_subtype).get.map(x => x._1 -> (x._2, x._3)).toMap

        val runid = "run_" + idx.toString
        logger.info("executing query for " + entityname.get + " (runid: " + runid + ")")
        val result = executeQuery(qo)
        logger.info("executed query for " + entityname.get + " (runid: " + runid + ")")

        //add all scans for comparison
        job.execution_subexecution.map(x => (x._1, scoring.get(x._1).map(_._1).getOrElse("unknown"), scoring.get(x._1).map(_._2).getOrElse(-1)))
          .foreach{ case(indexname, indextype, scanscore) =>
            val tmpQo = new RPCQueryObject(qo.id, "index", qo.options ++ Seq("indexname" -> indexname), qo.targets)
            val tmpResult = executeQuery(tmpQo) ++ Seq("scanscore" -> scanscore.toString)
            results += (runid + "-" + indextype -> tmpResult)
        }

        {
          val tmpQo = new RPCQueryObject(qo.id, "sequential", qo.options, qo.targets)
          val tmpResult = executeQuery(tmpQo) ++ Seq("scanscore" -> scoring.get(entityname.get).map(_._2).getOrElse(-1).toString)
          results += (runid + "-" + "sequential" -> tmpResult)
        }


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
}
