package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.{CreationHelper, Helpers}
import org.vitrivr.adampro.communication.datastructures._

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

    val nqueries = if (job.execution_nqueries.isEmpty) {
      None
    } else {
      Some(job.execution_nqueries.toInt)
    }

    val nruns = if (job.execution_nruns.isEmpty) {
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

    val executions = job.execution_subexecution.map(x => (x._1.toLowerCase, x._2))

    //query execution
    queries.zipWithIndex.foreach { case (qo, idx) =>
      if (running) {
        val runid = "run_" + idx.toString
        logger.info("executing query for " + entityname.get + " (runid: " + runid + ")")

        val scoredPaths = client.getScoredQueryExecutionPaths(qo, job.execution_subtype).get
        val scoring = scoredPaths.map(x => (x._1, x._2, x._3))
          .groupBy(_._2).mapValues(_.sortBy(_._3).reverse.head).toMap
        //index name, type, score; take only maximum

        val result = executeQuery(qo)

        //execute all other scans for comparison
        executions.map { case (indextype, withsequential) => (indextype, scoring.get(indextype.toLowerCase)) }.filter(_._2.isDefined)
          .filterNot(x => x._1 == "sequential")
          .foreach { case (indextype, scoring) =>
            val tmpQo = new RPCIndexScanQueryObject(qo.id, qo.options ++ Seq("indexname" -> scoring.get._1))
            val tmpResult = executeQuery(tmpQo) ++ Seq("scanscore" -> scoring.get._3.toString)

            if (job.measurement_firstrun && idx < NFIRST_RUN_QUERIES) {
              //ignore first run
            } else {
              results += (runid + "-loosers-" + indextype -> tmpResult)
            }


          }

        if (executions.map(_._1).contains("sequential")) {
          val tmpQo = new RPCSequentialScanQueryObject(qo.id, qo.options)
          val tmpResult = executeQuery(tmpQo) ++ Seq("scanscore" -> scoring.get("sequential").map(_._3).getOrElse(-1).toString)

          if (job.measurement_firstrun && idx < NFIRST_RUN_QUERIES) {
            //ignore first run
          } else {
            results += (runid + "-loosers-" + "sequential" -> tmpResult)
          }
        }

        logger.info("executed query for " + entityname.get + " (runid: " + runid + ")")

        if (job.measurement_firstrun && idx < NFIRST_RUN_QUERIES) {
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
  override protected def getQuery(entityname: String, k: Int, sparseQuery: Boolean, options: Seq[(String, String)] = Seq()): RPCGenericQueryObject = {
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

    lb.append("query" -> generateFeatureVector(job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max).mkString(","))

    if (sparseQuery) {
      lb.append("sparsequery" -> "true")
    }

    if (!job.execution_withsequential) {
      lb.append("indexonly" -> "true")
    }

    lb.append("informationlevel" -> "minimal")

    //confidence level
    lb.append("confidence" -> job.execution_confidence)

    lb.append("hints" -> job.execution_hint)

    lb.append(("nofallback" -> "true"))

    RPCEmpiricalScanQueryObject(id, (options ++ lb).toMap)
  }

  /**
    *
    * @param results
    * @return
    */
  override protected def prepareResults(results: ListBuffer[(String, Map[String, String])]) = {
    val props = super.prepareResults(results)

    val executions = job.execution_subexecution.map(x => (x._1.toLowerCase, x._2))

    props.setProperty("summary_desc", executions.map(_._1).mkString(","))

    executions.foreach { case(execution, withSequential) =>
      val filteredForExecution = results.filter(_._1.contains(execution)).map(_._2)
      val times = filteredForExecution.map(_.get("totaltime").getOrElse("-1"))
      val scanscores = filteredForExecution.map(_.get("scanscore").getOrElse("-1"))

      props.setProperty("summary_totaltimes_" + execution, times.mkString(","))
      props.setProperty("summary_scanscores_" + execution, scanscores.mkString(","))
      props.setProperty("summary_expectedtimes_" + execution, scanscores.map(score => 1.toDouble / score.toDouble).mkString(","))

      val metrics = filteredForExecution.flatMap(_.keySet.filter(_.contains("resultquality-")).map(_.replace("resultquality-", ""))).toSet

      metrics.foreach{ metric =>
        val qualitymeasures = filteredForExecution.map(_.get("resultquality-" + metric).getOrElse("-1"))
        props.setProperty("summary_resultquality_" + metric + "_" + execution, qualitymeasures.mkString(","))
      }

      props.setProperty("summary_resultquality_measures_" + execution, metrics.map(metric => "summary_resultquality_" + metric + "_" + execution).mkString(","))
    }

    props
  }
}
