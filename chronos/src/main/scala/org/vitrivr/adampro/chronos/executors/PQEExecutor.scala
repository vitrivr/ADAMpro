package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.{CreationHelper, Helpers}
import org.vitrivr.adampro.communication.datastructures.{RPCComplexQueryObject, RPCGenericQueryObject, RPCQueryResults}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
class PQEExecutor(job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) extends Executor(job, setStatus, inputDirectory, outputDirectory) {
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
    logger.info("generating queries to execute on " + entityname.get)
    val queries = getQueries(entityname.get)

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
    * Executes a query.
    *
    * @param qo
    */
  override protected def executeQuery(qo: RPCGenericQueryObject): Map[String, String] = {
    val lb = new ListBuffer[(String, Any)]()
    val ress = new ListBuffer[(Try[RPCQueryResults], Long)]()

    lb ++= (job.getAllParameters())

    logger.fine("executing query with parameters: " + job.getAllParameters().mkString)

    lb += ("queryid" -> qo.id)
    lb += ("operation" -> qo.operation)
    lb += ("options" -> qo.options.mkString)
    lb += ("debugQuery" -> qo.buildQueryMessage.toString())


    var isCompleted = false
    val t1 = System.currentTimeMillis
    var t2 = System.currentTimeMillis - 1 //returning -1 on error

    //do progressive query
    client.doProgressiveQuery(qo,
      next = (res) => ({
        val t3 = System.currentTimeMillis() - t1
        ress += ((res, t3))
      }),
      completed = (id) => ({
        isCompleted = true
        t2 = System.currentTimeMillis
      }))


    while (!isCompleted) {
      Thread.sleep(1000)
    }

    ress.zipWithIndex.foreach { case (res, idx) =>
      if (res._1.isSuccess) {
        lb += (idx + "_confidence" -> res._1.get.confidence)
        lb += (idx + "_source" -> (res._1.get.info.getOrElse("scantype", "") + " " +  res._1.get.info.getOrElse("indextype", "")))
        lb += (idx + "_adamprotime" -> res._1.get.time)
        lb += (idx + "_measuredtime" -> res._1.get.time)
        lb += (idx + "_results" -> {
          res._1.get.results.map(r => (r.get("ap_id").getOrElse("-") + "," + r.get("ap_distance").getOrElse("-1"))).mkString("(", "),(", ")")
        })
      } else {
        lb += (res._1.get.source + "_failure" -> res._1.failed.get.getMessage)
      }
    }

    lb += ("totaltime" -> math.abs(t2 - t1).toString)
    lb += ("starttime" -> t1)
    lb += ("endtime" -> t2)

    lb.toMap.mapValues(_.toString)
  }

  /**
    *
    * @param results
    * @return
    */
  override protected def prepareResults(results: ListBuffer[(String, Map[String, String])]) = {
    //fill properties
    val prop = new Properties
    prop.setProperty("evaluation_mode", job.general_mode)

    results.foreach {
      case (runid, result) =>
        result.map {
          case (k, v) => (runid + "_" + k) -> v
        } //remap key
          .foreach {
          case (k, v) => prop.setProperty(k, v)
        } //set property
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
}
