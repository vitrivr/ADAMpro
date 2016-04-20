package ch.unibas.dmi.dbis.adam.storage.partitions

import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.{Index, IndexHandler}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.log4j.Logger

import scala.util.Try

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object PartitionHandler {
  type PartitionID = Int

  val log = Logger.getLogger(getClass.getName)


  /**
    *
    * @param indexname
    * @param n
    * @param useMetadataForPartitioning
    * @param cols
    * @param option
    * @return
    */
  def repartitionIndex(indexname: IndexName, n: Int, useMetadataForPartitioning: Boolean, cols: Option[Seq[String]], option : PartitionOptions.Value)(implicit ac: AdamContext): Try[Index] = {
    val index = IndexHandler.load(indexname)

    if (index.isFailure) {
      log.error("index does not exist")
      throw IndexNotExistingException()
    }

    repartitionIndex(index.get, n, useMetadataForPartitioning, cols, option)
  }

  /**
    *
    * @param index
    * @param n
    * @param useMetadataForPartitioning
    * @param cols
    * @param option
    * @return
    */
  def repartitionIndex(index: Index, n: Int, useMetadataForPartitioning: Boolean, cols: Option[Seq[String]], option : PartitionOptions.Value)(implicit ac: AdamContext): Try[Index] = {
    val entity = EntityHandler.load(index.entityname)

    val join = if (useMetadataForPartitioning) {
      entity.get.getMetadata
    } else {
      None
    }

    IndexHandler.repartition(index, n, join, cols, option)
  }
}



/**
  *
  */
object PartitionOptions extends Enumeration {
  val CREATE_NEW = Value("create new index (materialize)")
  val REPLACE_EXISTING = Value("replace existing index (materialize)")
  val CREATE_TEMP = Value("create temporary index in cache")
}
