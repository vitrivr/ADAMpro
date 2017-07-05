package org.vitrivr.adampro.shared.transaction

import org.apache.spark.util.LongAccumulator
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

import scala.collection.mutable

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2017
  */
class VersionManager()(@transient implicit val ac: SharedComponentContext) extends Serializable with Logging {
  private val entityVersion = mutable.Map[String, LongAccumulator]()


  // entity
  def containsEntity(entityname : EntityName): Boolean = entityVersion.contains(entityname.toString)

  def put(entityname: EntityName) : Long = {
    val accum = ac.sc.longAccumulator(entityname.toString + "-version")
    entityVersion.+=(entityname.toString -> accum)
    accum.value
  }

  def getEntity(entityname : EntityName): LongAccumulator = entityVersion.get(entityname.toString).get
}

object VersionManager {

  /**
    * Create cache manager and fill it
    * @return
    */
  def build()(implicit ac: SharedComponentContext): VersionManager = new VersionManager()(ac)

}
