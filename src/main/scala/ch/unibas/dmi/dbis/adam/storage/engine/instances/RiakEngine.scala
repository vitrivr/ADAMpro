package ch.unibas.dmi.dbis.adam.storage.engine.instances

import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.KeyValueEngine
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class RiakEngine extends KeyValueEngine with Logging with Serializable {
  //private def getBucket(bucketname : String) = new Namespace(bucketname)


  /**
    * Create the entity in the key-value store.
    *
    * @param bucketname name of bucket to store feature to
    * @param attributes attributes of the entity
    * @return true on success
    */
  override def create(bucketname: String, attributes: Seq[AttributeDefinition])(implicit ac: AdamContext): Try[Option[String]] = ???

  /**
    *
    * @param bucketname name of bucket to check if bucket exists
    * @return
    */
  override def exists(bucketname: String)(implicit ac: AdamContext): Try[Boolean] = ???
  //val bucket = getBucket(bucketname)
  //val empty = ac.sc.riakBucket[String](bucket).queryAll().isEmpty()
  //Success(!empty)


  /**
    * Read entity from key-value store.
    *
    * @param bucketname name of bucket to read features from
    * @return
    */
  override def read(bucketname: String)(implicit ac: AdamContext): Try[DataFrame] = ???

  /**
    * Write entity to the key-value store.
    *
    * @param bucketname name of bucket to write features to
    * @param df         data
    * @param mode       save mode (append, overwrite, ...)
    * @return true on success
    */
  override def write(bucketname: String, df: DataFrame, mode: SaveMode)(implicit ac: AdamContext): Try[Void] = ???

  /**
    * Drop the entity from the key-value store.
    *
    * @param bucketname name of bucket to be dropped
    * @return true on success
    */
  override def drop(bucketname: String)(implicit ac: AdamContext): Try[Void] = ???

}
