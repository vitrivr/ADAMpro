package org.vitrivr.adampro.main

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.{ReentrantReadWriteLock, StampedLock}

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.SparkContext
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.entity.EntityLRUCache
import org.vitrivr.adampro.query.optimizer.OptimizerRegistry
import org.vitrivr.adampro.index.IndexLRUCache
import org.vitrivr.adampro.query.QueryLRUCache
import org.vitrivr.adampro.storage.StorageHandlerRegistry

import scala.annotation.implicitNotFound
import scala.collection.mutable

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
@implicitNotFound("Cannot find an implicit AdamContext, either import SparkStartup.Implicits._ or use a custom one")
trait AdamContext extends Serializable {
  val spark : SparkSession
  def sc : SparkContext
  def sqlContext : SQLContext

  val storageHandlerRegistry = new StorageHandlerRegistry()

  val entityLRUCache = new EntityLRUCache()(this)
  val entityVersion = mutable.Map[String, LongAccumulator]()

  private val entityLocks = new ConcurrentHashMap[String, StampedLock]()

  def getEntityLock(entityname: EntityName) = synchronized {
    val newLock = new StampedLock()
    val res = entityLocks.putIfAbsent(entityname.toString, newLock)

    val ret = entityLocks.get(entityname.toString)

    if(ret == null){
      newLock
    } else {
      ret
    }
  }


  val indexLRUCache = new IndexLRUCache()(this)

  val queryLRUCache = new QueryLRUCache()(this)

  val optimizerRegistry = sc.broadcast(new OptimizerRegistry())

  val config : AdamConfig
}