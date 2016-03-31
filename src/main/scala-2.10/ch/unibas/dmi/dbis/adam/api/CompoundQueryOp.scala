package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.query.handler.CompoundQueryHandler
import ch.unibas.dmi.dbis.adam.query.handler.CompoundQueryHandler.Expression
import org.apache.spark.sql.DataFrame

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object CompoundQueryOp {
  /**
    *
    * @param e
    * @param indexOnly
    * @param withMetadata
    * @return
    */
  def apply(e: Expression, indexOnly: Boolean = false, withMetadata: Boolean = false) : DataFrame = {
    CompoundQueryHandler(e, indexOnly, withMetadata)
  }

}


