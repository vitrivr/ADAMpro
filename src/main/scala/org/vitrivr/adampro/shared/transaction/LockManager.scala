package org.vitrivr.adampro.shared.transaction

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.StampedLock

import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * July 2017
  */
class LockManager() extends Serializable with Logging {
  private val entityLocks = new ConcurrentHashMap[String, StampedLock]()

  def getLockEntity(entityname: EntityName) = synchronized {
    val newLock = new StampedLock()
    val res = entityLocks.putIfAbsent(entityname.toString, newLock)

    val ret = entityLocks.get(entityname.toString)

    if(ret == null){
      newLock
    } else {
      ret
    }
  }
}

object LockManager {
  /**
    * Create lock manager and fill it
    * @return
    */
  def build()(implicit ac: SharedComponentContext): LockManager = new LockManager()
}
