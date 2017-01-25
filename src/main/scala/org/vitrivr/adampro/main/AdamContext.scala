package org.vitrivr.adampro.main

import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{Accumulator, SparkContext}
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.entity.EntityLRUCache
import org.vitrivr.adampro.helpers.optimizer.OptimizerRegistry
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

  val storageHandlerRegistry = sc.broadcast(new StorageHandlerRegistry())

  val entityLRUCache = new EntityLRUCache()(this)
  val entityVersion = mutable.Map[String, LongAccumulator]()

  val indexLRUCache = new IndexLRUCache()(this)

  val queryLRUCache = new QueryLRUCache()(this)

  val optimizerRegistry = sc.broadcast(new OptimizerRegistry())

  val config : AdamConfig
}