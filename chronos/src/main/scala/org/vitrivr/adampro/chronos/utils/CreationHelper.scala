package org.vitrivr.adampro.chronos.utils

import java.util.logging.Logger

import org.vitrivr.adampro.chronos.EvaluationJob
import org.vitrivr.adampro.rpc.RPCClient
import org.vitrivr.adampro.rpc.datastructures.RPCAttributeDefinition

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * April 2017
  */
object CreationHelper {
  val logger: Logger = Logger.getLogger(this.getClass.getName)

  private val ENTITY_NAME_PREFIX = "chron_eval_"
  protected val FEATURE_VECTOR_ATTRIBUTENAME = "vector"

  /**
    *
    * @param client
    * @param job
    * @return
    */
  def createEntity(client: RPCClient, job: EvaluationJob): Try[String] = {
    try {
      val entityname = if (client.entityExists(generateEntityname(job)).get && job.data_enforcecreation) {
        //generate a new entity with a random name
        Helpers.generateString(10)
      } else {
        //get entity based on creation attributes
        generateEntityname(job)
      }

      val attributename = job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME)

      val attributes = getAttributeDefinition(job)

      var entityCreatedNewly = false

      //create entity
      if (client.entityExists(entityname).get) {
        logger.info("entity " + entityname + " exists already")
        entityCreatedNewly = false
      } else {
        logger.info("creating entity " + entityname + " (" + attributes.map(a => a.name + "(" + a.datatype + ")").mkString(",") + ")")

        val entityCreatedRes = client.entityCreate(entityname, attributes)

        if (entityCreatedRes.isFailure) {
          logger.severe(entityCreatedRes.failed.get.getMessage)
          throw entityCreatedRes.failed.get
        }

        //insert random data
        logger.info("inserting " + job.data_tuples + " tuples into " + entityname)
        client.entityGenerateRandomData(entityname, job.data_tuples, job.data_vector_dimensions, job.data_vector_sparsity, job.data_vector_min, job.data_vector_max, Some(job.data_vector_distribution))

        entityCreatedNewly = true
      }

      //vacuuming before doing query
      client.entityVacuum(entityname)

      if (job.measurement_cache) {
        client.entityCache(entityname)
      }

      Success(entityname)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param job
    * @return
    */
  private def getAttributeDefinition(job: EvaluationJob): Seq[RPCAttributeDefinition] = {
    val lb = new ListBuffer[RPCAttributeDefinition]()

    //vector
    lb.append(RPCAttributeDefinition(job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME), "vector"))

    //metadata
    val metadata = Map("long" -> job.data_metadata_long, "int" -> job.data_metadata_int,
      "float" -> job.data_metadata_float, "double" -> job.data_metadata_double,
      "string" -> job.data_metadata_string, "text" -> job.data_metadata_text,
      "boolean" -> job.data_metadata_boolean
    )

    metadata.foreach { case (datatype, number) =>
      (0 until number).foreach { i =>
        lb.append(RPCAttributeDefinition(datatype + "i", datatype, Some("parquet")))
      }
    }

    lb.toSeq
  }

  /**
    *
    * @param client
    * @param job
    * @param entityname
    * @return
    */
  def createIndexes(client: RPCClient, job: EvaluationJob, entityname: String): Try[Seq[String]] = {
    try {
      val attributename = job.data_attributename.getOrElse(FEATURE_VECTOR_ATTRIBUTENAME)

      //vacuuming before doing query
      client.entityVacuum(entityname)

      var indexCreatedNewly = false

      val indexnames = if (job.execution_name == "sequential") {
        //no index
        logger.info("creating no index for " + entityname)
        Seq()
      } else if (job.execution_name == "progressive") {
        logger.info("creating all indexes for " + entityname)
        indexCreatedNewly = true
        client.entityCreateAllIndexes(entityname, Seq(attributename), 2).get
      } else if(job.execution_name == "stochastic" || job.execution_name == "empirical") {
        job.execution_subexecution.flatMap{ case(indextype, withsequential) =>
          if (client.indexExists(entityname, attributename, indextype).get) {
            logger.info(indextype + " index for " + entityname + " (" + attributename + ") " + "exists already")
            indexCreatedNewly = false
            client.indexList(entityname).get.filter(_._2 == attributename).filter(_._3 == indextype).map(_._1)
          } else {
            logger.info("creating " + indextype + " index for " + entityname)
            indexCreatedNewly = true
            Seq(client.indexCreate(entityname, attributename, indextype, 2, Map()).get)
          }
        }
      } else {
        if (client.indexExists(entityname, attributename, job.execution_subtype).get) {
          logger.info(job.execution_subtype + " index for " + entityname + " (" + attributename + ") " + "exists already")
          indexCreatedNewly = false
          client.indexList(entityname).get.filter(_._2 == attributename).filter(_._3 == job.execution_subtype).map(_._1)
        } else {
          logger.info("creating " + job.execution_subtype + " index for " + entityname)
          indexCreatedNewly = true
          Seq(client.indexCreate(entityname, attributename, job.execution_subtype, 2, Map()).get)
        }
      }

      if (job.measurement_cache) {
        indexnames.foreach { indexname =>
          client.indexCache(indexname)
        }
      }

      Success(indexnames)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param job
    * @return
    */
  private def generateEntityname(job: EvaluationJob): String = {
    val prime = 31
    var result = 1
    result = prime * result + job.data_tuples.hashCode
    result = prime * result + job.data_vector_dimensions.hashCode
    result = prime * result + job.data_vector_min.hashCode
    result = prime * result + job.data_vector_max.hashCode
    result = prime * result + job.data_vector_distribution.hashCode
    result = prime * result + job.data_vector_sparsity.hashCode
    result = prime * result + job.data_metadata_boolean.hashCode
    result = prime * result + job.data_metadata_double.hashCode
    result = prime * result + job.data_metadata_float.hashCode
    result = prime * result + job.data_metadata_int.hashCode
    result = prime * result + job.data_metadata_string.hashCode
    result = prime * result + job.data_metadata_long.hashCode
    result = prime * result + job.data_metadata_text.hashCode

    ENTITY_NAME_PREFIX + result.toString.replace("-", "m")
  }
}
