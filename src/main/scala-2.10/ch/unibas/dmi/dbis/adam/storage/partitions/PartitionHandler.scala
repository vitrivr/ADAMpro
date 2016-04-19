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
    * @param materialize
    * @param replace
    * @param ac
    */
  def repartitionIndex(indexname: IndexName, n: Int, useMetadataForPartitioning: Boolean, cols: Option[Seq[String]], materialize: Boolean = false, replace: Boolean = false)(implicit ac: AdamContext): Try[Index] = {
    val index = IndexHandler.load(indexname)

    if (index.isFailure) {
      log.error("index does not exist")
      throw IndexNotExistingException()
    }

    val entity = EntityHandler.load(index.get.entityname)

    val join = if(useMetadataForPartitioning){
      entity.get.getMetadata
    } else {
      None
    }

    IndexHandler.repartition(index.get, n, join, cols, materialize, replace)
  }
}
