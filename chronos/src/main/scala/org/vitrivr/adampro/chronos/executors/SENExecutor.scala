package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.CreationHelper
import org.vitrivr.adampro.grpc.grpc.RepartitionMessage

import scala.collection.mutable.ListBuffer

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  *
  *
  * Executor for standard evaluation.
  */
class SENExecutor(job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) extends Executor(job, setStatus, inputDirectory, outputDirectory) {
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

    //partition
    getPartitionCombinations().foreach { case (e, i) =>
      if (e.isDefined) {
        if (RepartitionMessage.Partitioner.values.find(p => p.name == job.access_entity_partitioner).isDefined) {
          client.entityPartition(entityname.get, e.get, None, true, true, job.access_index_partitioner)
        } else client.entityPartition(entityname.get, e.get, None, true, true)
        //TODO: add partition column to job
      }

      if (i.isDefined) {
        //TODO: add partition column to job
        if (RepartitionMessage.Partitioner.values.find(p => p.name == job.access_index_partitioner).isDefined) {
          indexnames.get.foreach(indexname => client.indexPartition(indexname, i.get, None, true, true, job.access_index_partitioner))
        } else indexnames.get.foreach(indexname => client.indexPartition(indexname, i.get, None, true, true))
      }


      val options = ListBuffer[(String, String)]()
      if(indexnames.get.length == 1){
        options += "indexname" -> indexnames.get.head
      }

      //collect queries
      logger.info("generating queries to execute on " + entityname.get)
      val queries = getQueries(entityname.get, options.toList)

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
