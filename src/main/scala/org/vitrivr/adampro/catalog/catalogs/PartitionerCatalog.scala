package org.vitrivr.adampro.catalog.catalogs

import org.vitrivr.adampro.catalog.CatalogOperator
import slick.driver.DerbyDriver.api._

/**
  * ADAMpro
  *
  * Catalog for storing partitioning information
  *
  * Silvan Heller
  * August 2016
  */
private[catalog] class PartitionerCatalog(tag: Tag) extends Table[(String, Int, Array[Byte], Array[Byte])](tag, Some(CatalogOperator.SCHEMA), "ap_partitioner") {
  def indexname = column[String]("index") //index or entity this partitioner belongs to

  def noPartitions = column[Int]("noPartitions") //number of partitions

  def meta = column[Array[Byte]]("meta") //metadata about this partitioner

  def partitioner = column[Array[Byte]]("partitioner")


  /**
    * Special fields
    */
  override def * = (indexname, noPartitions, meta, partitioner)

  def index = foreignKey("partitioner_index_fk", indexname, TableQuery[IndexCatalog])(_.indexname, onUpdate=ForeignKeyAction.Restrict, onDelete=ForeignKeyAction.Cascade)
}