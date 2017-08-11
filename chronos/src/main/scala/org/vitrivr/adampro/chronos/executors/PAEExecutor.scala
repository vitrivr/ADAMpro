package org.vitrivr.adampro.chronos.executors

import java.io.File
import java.util.Properties

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.chronos.utils.{CreationHelper, Helpers}
import org.vitrivr.adampro.communication.datastructures.{RPCComplexQueryObject, RPCGenericQueryObject, RPCQueryResults, RPCSequentialScanQueryObject}

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
class PAEExecutor(job: EvaluationJob, setStatus: (Double) => (Boolean), inputDirectory: File, outputDirectory: File) extends Executor(job, setStatus, inputDirectory, outputDirectory) {
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
    * Gets single query
    *
    * @param entityname
    * @param k
    * @param sparseQuery
    * @param options
    * @return
    */
  override protected def getQuery(entityname: String, k: Int, sparseQuery: Boolean, options : Seq[(String, String)] = Seq()): RPCComplexQueryObject = {
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

    lb.append("hints" -> job.execution_subexecution.map(_._1.toLowerCase).mkString(","))

    RPCComplexQueryObject(Helpers.generateString(10), (options ++ lb).toMap, job.execution_name, None)
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

    //do parallel query
    client.doParallelQuery(qo,
      next = (res) => ({
        if(res.isSuccess){
          val t3 = System.currentTimeMillis() - t1
          ress += ((res, t3))
        } else {
          logger.warning("error in executing parallel querying: " + res.failed.get.getMessage)
          isCompleted = true
        }
      }),
      completed = (id) => ({
        isCompleted = true
        t2 = System.currentTimeMillis
      }))


    while (!isCompleted) {
      Thread.sleep(1000)
    }

    ress.foreach { case (res, time) =>
      if (res.isSuccess) {
        lb += (res.get.source + "_confidence" -> res.get.confidence)
        lb += (res.get.source + "_source" -> res.get.source)
        lb += (res.get.source + "_adamprotime" -> res.get.time)
        lb += (res.get.source + "_measuredtime" -> time)
        lb += (res.get.source + "_results" -> {
          res.get.results.map(res => (res.get("ap_id").getOrElse("-") + "," + res.get("ap_distance").getOrElse("-1"))).mkString("(", "),(", ")")
        })
      } else {
        lb += (res.get.source + "_failure" -> res.failed.get.getMessage)
      }
    }


    if (job.measurement_resultquality) {
      //perform sequential query
      val opt = collection.mutable.Map() ++ qo.options
      opt -= "hints"
      opt += "hints" -> "sequential"
      val gtruth = client.doQuery(RPCSequentialScanQueryObject(qo.id, opt.toMap))

      if (gtruth.isSuccess) {
        val gtruthPKs = gtruth.get.map(_.results.map(_.get("ap_id"))).head.map(_.get)

        ress.foreach { case (res, time) =>
          if (res.isSuccess) {

            val resPKs = res.get.results.map(_.get("ap_id").get)

            val agreements = gtruthPKs.intersect(resPKs).length
            //simple hits/total
            val quality = (agreements / qo.options.get("k").get.toDouble)
            lb += (res.get.source + "_resultquality" -> quality.toString)
          } else {
            lb += (res.get.source + "_resultquality" -> gtruth.failed.get.getMessage)
          }
        }
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


    prop.setProperty("summary_data_vector_dimensions", job.data_vector_dimensions.toString)
    prop.setProperty("summary_data_tuples", job.data_tuples.toString)

    prop.setProperty("summary_execution_name", job.execution_name)
    prop.setProperty("summary_execution_subtype", job.execution_subexecution.map(_._1).mkString(", "))


    //get overview for plotting
    val summary = results.zipWithIndex.map {
      case (result, runid) => {
        val times = result._2.filter(_._1.endsWith("_measuredtime")).map { case (desc, time) => (desc.replace("_measuredtime", ""), time.toLong) }.toMap
        val qualities = result._2.filter(_._1.endsWith("_resultquality")).map { case (desc, res) => (desc.replace("_resultquality", ""), res.toDouble) }.toMap

        val descLb = new ListBuffer[String]()
        val timeLb = new ListBuffer[Long]()
        val qualityLb = new ListBuffer[Double]()

        val ress = times.keys.map { key =>
          descLb += key
          timeLb += times.get(key).get
          qualityLb += qualities.get(key).getOrElse(-1.0)
        }


        prop.setProperty("summary_desc_" + runid, descLb.mkString(","))
        prop.setProperty("summary_totaltime_" + runid, timeLb.mkString(","))
        prop.setProperty("summary_resultquality_" + runid, qualityLb.mkString(","))

      }
    }

    prop.setProperty("summary_runs", summary.length.toString)

    prop
  }
}
