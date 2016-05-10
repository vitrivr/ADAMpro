package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.exception.IndexNotExistingException
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.{Index, IndexHandler}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.partitions.{PartitionHandler, PartitionOptions}

import scala.util.{Failure, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
object PartitionOp {

  /**
    *
    * @param indexname
    * @param n
    * @param useMetadataForPartitioning
    * @param cols
    * @param option
    * @return
    */
  def apply(indexname: IndexName, n: Int, useMetadataForPartitioning: Boolean, cols: Option[Seq[String]], option: PartitionOptions.Value)(implicit ac: AdamContext): Try[Index] = {
    try {
      val index = IndexHandler.load(indexname)

      if (index.isFailure) {
        throw IndexNotExistingException()
      }

      PartitionHandler.repartitionIndex(index.get, n, useMetadataForPartitioning, cols, option)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
