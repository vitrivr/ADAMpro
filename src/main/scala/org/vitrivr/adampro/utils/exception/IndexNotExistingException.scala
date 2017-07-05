package org.vitrivr.adampro.utils.exception

import org.vitrivr.adampro.data.index.Index.IndexName
/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
case class IndexNotExistingException(message : String = "Index not existing.")  extends GeneralAdamException(message)

object IndexNotExistingException {
  def withIndexname(indexname : IndexName): IndexNotExistingException = new IndexNotExistingException(s"Index '$indexname' not existing.")
}
