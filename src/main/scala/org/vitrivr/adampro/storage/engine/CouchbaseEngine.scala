package org.vitrivr.adampro.storage.engine

import java.io.Serializable

import com.couchbase.client.java.bucket.BucketType
import com.couchbase.client.java.cluster.DefaultBucketSettings
import com.couchbase.client.java.query.{Index, N1qlQuery}
import com.couchbase.spark.connection.{CouchbaseConfig, CouchbaseConnection}
import com.couchbase.spark.sql._
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.exception.GeneralAdamException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2017
  */
@Experimental class CouchbaseEngine(url: String, user: String, password: String)(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Serializable {
  override val name = "couchbase"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.STRINGTYPE, AttributeTypes.VECTORTYPE)

  override def specializes = Seq(AttributeTypes.VECTORTYPE)

  override val repartitionable = false


  private val cfg = CouchbaseConfig(ac.sc.getConf.set("spark.couchbase.nodes", url))
  private val cluster = CouchbaseConnection().cluster(cfg)

  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this(props.get("url").get, props.get("user").get, props.get("password").get)(ac)
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
    try {
      val bucketSettings = new DefaultBucketSettings.Builder()
        .`type`(BucketType.COUCHBASE)
        .name(storename)
        .quota(100) //get as param?
        .build()

      cluster.clusterManager(user, password).insertBucket(bucketSettings)

      val bucket = cluster.openBucket(storename)

      //create primary index
      bucket.query(N1qlQuery.simple( Index.createPrimaryIndex().on(storename)))

      Success(Map())
    } catch {
      case e: Exception =>
        log.error("fatal error when creating bucket in couchbase", e)
        Failure(e)
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
      Success(cluster.clusterManager(user, password).hasBucket(storename))

    } catch {
      case e: Exception =>
        log.error("fatal error when checking for existence in couchbase", e)
        Failure(e)
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
    try {

      val structFields = attributes.map {
        attribute => StructField(attribute.name, attribute.attributeType.datatype)
      }

      //TODO: how to dynamically add bucket to Spark config?
      ac.sc.getConf.set("spark.couchbase.bucket." + storename, "")
      val data = ac.spark.read.couchbase(StructType(structFields), Map("bucket" -> storename))

      Success(data)
    } catch {
      case e: Exception =>
        log.error("fatal error when reading from couchbase", e)
        Failure(e)
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
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try {
      /*if (mode != SaveMode.Append) {
        throw new UnsupportedOperationException("only appending is supported")
      }*/
      val pkattribute = attributes.filter(_.pk)
      assert(pkattribute.size <= 1)

      df.withColumn("couchbase-id", col(pkattribute.head.name)).write.couchbase(Map("bucket" -> storename, "password" -> "", "idField" -> "couchbase-id"))

      Success(Map())
    } catch {
      case e: Exception =>
        log.error("fatal error when writing to couchbase", e)
        Failure(e)
    }
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
    try {
      val res = cluster.clusterManager(user, password).removeBucket(storename)

      if(res){
        Success(null)
      } else {
        throw new GeneralAdamException("could not remove bucket")
      }
    } catch {
      case e: Exception =>
        log.error("fatal error when dropping from couchbase", e)
        Failure(e)
    }
  }
}
