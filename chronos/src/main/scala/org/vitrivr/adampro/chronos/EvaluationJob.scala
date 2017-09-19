package org.vitrivr.adampro.chronos

import ch.unibas.dmi.dbis.chronos.agent.ChronosJob

import scala.collection.mutable.ListBuffer
import scala.xml.{Node, Text, XML}


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2016
  */
class EvaluationJob(job: ChronosJob) extends ChronosJob(job) {
  private val xml = XML.loadString(job.cdl)
  private val general = (xml \ "evaluation" \ "general").head
  private val adampro = (xml \ "evaluation" \ "adampro").head
  private val data = (xml \ "evaluation" \ "data").head
  private val query = (xml \ "evaluation" \ "query").head
  private val execution = (xml \ "evaluation" \ "execution").head
  private val access = (xml \ "evaluation" \ "access").head
  private val measurement = (xml \ "evaluation" \ "measurement").head
  private val maintenance = (xml \ "evaluation" \ "maintenance").head

  //attention: add all parameters that you have here also to the method getAllParameters, to ensure
  //that the parameters are properly logged

  //evaluation
  val general_mode : String = getAttribute(general, "mode")

  //adampro
  val adampro_url: String = getAttribute(adampro, "url")
  val adampro_port: Int = getAttribute(adampro, "port").toInt

  //data parameters
  val data_entityname: Option[String] = {
    val entityname = getAttribute(data, "entityname", false)

    if (entityname.isEmpty) {
      None
    } else {
      Some(entityname)
    }
  }
  val data_attributename: Option[String] = {
    val attributename = getAttribute(data, "attributename", false)

    if (attributename.isEmpty) {
      None
    } else {
      Some(attributename)
    }
  }
  val data_enforcecreation: Boolean = getBooleanAttribute(data, "enforcecreation", false)

  val data_tuples: Int = getAttribute(data, "tuples").toInt
  val data_vector_dimensions: Int = getAttribute(data, "vector_dimensions").toInt
  val data_vector_sparsity: Float = getAttribute(data, "vector_sparsity").toFloat
  val data_vector_min: Float = getAttribute(data, "vector_min").toFloat
  val data_vector_max: Float = getAttribute(data, "vector_max").toFloat
  val data_vector_distribution: String = getAttribute(data, "vector_distribution")

  val data_metadata_long: Int = getAttribute(data, "metadata_long").toInt
  val data_metadata_int: Int = getAttribute(data, "metadata_int").toInt
  val data_metadata_float: Int = getAttribute(data, "metadata_float").toInt
  val data_metadata_double: Int = getAttribute(data, "metadata_double").toInt
  val data_metadata_string: Int = getAttribute(data, "metadata_string").toInt
  val data_metadata_text: Int = getAttribute(data, "metadata_text").toInt
  val data_metadata_boolean: Int = getAttribute(data, "metadata_boolean").toInt

  val data_storagehandler : String = getAttribute(data, "storagehandler")

  //query parameters
  val query_k: Seq[Int] = getAttribute(query, "k").split(",").map(_.toInt)
  val query_n: Int = getAttribute(query, "n").toInt
  val query_distance: String = getAttribute(query, "distance")
  val query_weighted: Boolean = getBooleanAttribute(query, "weighted")

  val query_path : Option[String] = {
    if(getAttribute(query, "path", false).trim.length < 1){
      None
    } else {
      Some(getAttribute(query, "path", false))
    }
  }

  //execution paths
  val execution_name: String = getAttribute(execution, "name")
  val execution_subtype: String = getAttribute(execution, "subtype")
  val execution_withsequential: Boolean = getBooleanAttribute(execution, "withsequential")
  val execution_hint: String = getAttribute(execution, "hint")

  val execution_nqueries: String = getAttribute(execution, "nqueries", false)
  val execution_nruns: String = getAttribute(execution, "nruns", false)
  val execution_confidence: String = getAttribute(execution, "confidence", false)


  val execution_subexecution : Seq[(String, Boolean)] = if(!isEmpty(execution)){
    val execution_subexecutions = (xml \ "evaluation" \ "execution" \ "subexecutions" \ "subexecution")

    execution_subexecutions.map{ subexecution =>
      val subexecution_name = getAttribute(subexecution, "name")
      val subexecution_withsequential = getBooleanAttribute(subexecution, "withsequential")

      (subexecution_name, subexecution_withsequential)
    }
  } else {
    Seq()
  }

  //data access parameters
  val access_entity_partitions: Seq[Int] = getAttribute(access, "entity_partitions").split(",").filterNot(s => s.length < 0 || s.isEmpty).map(_.toInt)
  val access_entity_partitioner: String = getAttribute(access, "entity_partitioner", false)
  val access_index_partitions: Seq[Int] = getAttribute(access, "index_partitions").split(",").filterNot(s => s.length < 0 || s.isEmpty).map(_.toInt)
  val access_index_partitioner: String = getAttribute(access, "index_partitioner", false)

  //measurement parameters
  val measurement_resultquality: Boolean = getBooleanAttribute(measurement, "resultquality")
  val measurement_firstrun: Boolean = getBooleanAttribute(measurement, "firstrun")
  val measurement_cache: Boolean = getBooleanAttribute(measurement, "cache")

  //maintenance parameters
  val maintenance_delete : Boolean = getBooleanAttribute(maintenance, "delete")

  /**
    *
    * @param node
    * @return
    */
  private def isEmpty(node : Node) : Boolean = {
    node.child.filter {childNode => !childNode.isInstanceOf[Text] || !childNode.text.trim.isEmpty}.isEmpty
  }

  /**
    *
    * @param node
    * @param key
    * @param errorIfEmpty
    * @return
    */
  private def getAttribute(node: Node, key: String, errorIfEmpty: Boolean = true): String = {
    val attributeNode = node.attribute(key)

    if (errorIfEmpty && attributeNode.isEmpty) {
      throw new Exception("attribute " + key + " is missing")
    } else if (attributeNode.isEmpty) {
      ""
    } else {
      attributeNode.get.text
    }
  }

  /**
    *
    * @param node
    * @param key
    * @param errorIfEmpty
    * @return
    */
  private def getBooleanAttribute(node: Node, key: String, errorIfEmpty: Boolean = true): Boolean = {
    val result = getAttribute(node, key, errorIfEmpty)

    if (result.isEmpty || result.length == 0) {
      false
    } else {
      (result == "1")
    }
  }

  /**
    *
    * @return
    */
  def getAllParameters(): Map[String, String] = {
    val lb = new ListBuffer[(String, Any)]()

    lb += ("general_mode" -> general_mode)

    if(general_mode == "see"){
      lb += ("data_entityname" -> data_entityname.getOrElse("unknown"))
    }

    lb += ("data_enforcecreation" -> data_enforcecreation)
    lb += ("data_tuples" -> data_tuples)
    lb += ("data_vector_dimensions" -> data_vector_dimensions)
    lb += ("data_vector_sparsity" -> data_vector_sparsity)
    lb += ("data_vector_min" -> data_vector_min)
    lb += ("data_vector_max" -> data_vector_max)
    lb += ("data_vector_distribution" -> data_vector_distribution)
    lb += ("data_metadata_long" -> data_metadata_long)
    lb += ("data_metadata_int" -> data_metadata_int)
    lb += ("data_metadata_float" -> data_metadata_float)
    lb += ("data_metadata_double" -> data_metadata_double)
    lb += ("data_metadata_string" -> data_metadata_string)
    lb += ("data_metadata_text" -> data_metadata_text)
    lb += ("data_metadata_boolean" -> data_metadata_boolean)

    //query parameters
    lb += ("query_k" -> query_k)
    lb += ("query_n" -> query_n)
    lb += ("query_distance" -> query_distance)
    lb += ("query_weighted" -> query_weighted)

    //execution paths
    lb += ("execution_name" -> execution_name)
    lb += ("execution_withsequential" -> execution_withsequential)
    lb += ("execution_hint" -> execution_hint)
    lb += ("execution_subtype" -> execution_subtype)
    lb += ("execution_subexecution" -> execution_subexecution.map(x => x._1 + "_" + x._2).mkString(","))

    //data access parameters
    lb += ("access_entity_partitions" -> access_entity_partitions)
    lb += ("access_entity_partitioner" -> access_entity_partitioner)
    lb += ("access_index_partitions" -> access_index_partitions)
    lb += ("access_index_partitioner" -> access_index_partitioner)

    //measurement parameters
    lb += ("measurement_firstrun" -> measurement_firstrun)
    lb += ("measurement_cache" -> measurement_cache)

    lb.toMap.mapValues(_.toString)
  }
}
