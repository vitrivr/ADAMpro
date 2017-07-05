package org.vitrivr.adampro.process

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.process.SparkStartup.mainContext
import org.vitrivr.adampro.query.planner.PlannerRegistry
import org.vitrivr.adampro.shared.cache.CacheManager
import org.vitrivr.adampro.shared.catalog.{CatalogManager, LogManager}
import org.vitrivr.adampro.shared.transaction.{LockManager, VersionManager}
import org.vitrivr.adampro.storage.StorageManager

import scala.annotation.implicitNotFound

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
@implicitNotFound("Cannot find an implicit SharedComponentContext, either import SparkStartup.Implicits._ or use a custom one")
trait SharedComponentContext extends Serializable {
  val spark : SparkSession
  def sc : SparkContext
  def sqlContext : SQLContext

  val config = new AdamConfig()

  val catalogManager = CatalogManager.build()(this)

  val logManager = LogManager.build()(this)

  val cacheManager = CacheManager.build()(this)

  val versionManager = VersionManager.build()(this)

  val lockManager = LockManager.build()(this)

  val storageManager = StorageManager.build()(this)
}