package org.vitrivr.adampro.main

import org.apache.spark.sql.SQLContext
import org.apache.spark.{Accumulator, SparkContext}
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
trait AdamContext {
  def sc : SparkContext
  def sqlContext : SQLContext

  val storageHandlerRegistry = sc.broadcast(new StorageHandlerRegistry())

  val entityLRUCache = new EntityLRUCache()
  val entityVersion = mutable.Map[String, Accumulator[Long]]()

  val indexLRUCache = new IndexLRUCache()

  val queryLRUCache = new QueryLRUCache()

  val optimizerRegistry = sc.broadcast(new OptimizerRegistry())
}
