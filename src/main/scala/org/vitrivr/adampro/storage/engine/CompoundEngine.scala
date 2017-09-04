package org.vitrivr.adampro.storage.engine

import java.io.Serializable

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
@Experimental class CompoundEngine(fullAccessHandlerName : String, randomAccessHandlerName : String)(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Serializable {
  override val name: String = "compound"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.FLOATTYPE, AttributeTypes.DOUBLETYPE, AttributeTypes.STRINGTYPE, AttributeTypes.TEXTTYPE, AttributeTypes.BOOLEANTYPE, AttributeTypes.GEOGRAPHYTYPE, AttributeTypes.GEOMETRYTYPE, AttributeTypes.VECTORTYPE, AttributeTypes.SPARSEVECTORTYPE)

  override def specializes: Seq[AttributeTypes.AttributeType] = Seq()

  override val repartitionable: Boolean = true

  //set engines lazy!
  private lazy val fullAccessEngine = ac.storageManager.get(fullAccessHandlerName).get.engine
  private lazy val randomAccessEngine = ac.storageManager.get(randomAccessHandlerName).get.engine

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this(props.get("fullAccessHandler").get, props.get("randomAccessHandler").get)(ac)
  }

  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try{
    fullAccessEngine.create(storename, attributes, params)
    randomAccessEngine.create(storename, attributes, params)
    } catch {
      case e : Exception => Failure(e)
    }
  }

  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    try {
      Success(fullAccessEngine.exists(storename).get || randomAccessEngine.exists(storename).get)
    } catch {
      case e : Exception => Failure(e)
    }
  }

  /**
    * Read entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes the attributes to read
    * @param predicates filtering predicates (only applied if possible)
    * @param params     reading parameters
    * @return
    */
  override def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: SharedComponentContext): Try[DataFrame] = {
    if (predicates.nonEmpty) {
      randomAccessEngine.read(storename, attributes, predicates, params)
    } else {
      fullAccessEngine.read(storename, attributes, predicates, params)
    }
  }

  /**
    * Write entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param df         data
    * @param attributes attributes to store
    * @param mode       save mode (append, overwrite, ...)
    * @param params     writing parameters
    * @return new options to store
    */
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try{
      randomAccessEngine.write(storename, df, attributes, mode, params)
      fullAccessEngine.write(storename, df, attributes, mode, params)
    } catch {
      case e : Exception => Failure(e)
    }
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
    try{
      randomAccessEngine.drop(storename)
      fullAccessEngine.drop(storename)

      Success(null)
    } catch {
      case e : Exception => Failure(e)
    }
  }
}
