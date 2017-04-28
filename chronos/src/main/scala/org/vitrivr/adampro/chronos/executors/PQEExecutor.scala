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
    * Executes a query.
    *
    * @param qo
    */
  override protected def executeQuery(qo: RPCQueryObject): Map[String, String] = {
    val lb = new ListBuffer[(String, Any)]()

    lb ++= (job.getAllParameters())

    logger.fine("executing query with parameters: " + job.getAllParameters().mkString)

    lb += ("queryid" -> qo.id)
    lb += ("operation" -> qo.operation)
    lb += ("options" -> qo.options.mkString)
    lb += ("debugQuery" -> qo.getQueryMessage.toString())


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

    lb += ("totaltime" -> math.abs(t2 - t1).toString)
    lb += ("starttime" -> t1)
    lb += ("endtime" -> t2)


    lb.toMap.mapValues(_.toString)
  }
}
