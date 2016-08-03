package ch.unibas.dmi.dbis.adam.catalog.catalogs

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import slick.driver.PostgresDriver.api._

/**
  * Created by silvan on 03.08.16.
  */
private[catalog] class PartitionerCatalog(tag: Tag) extends Table[(String, Int, Array[Byte], Array[Byte])](tag, Some(CatalogOperator.SCHEMA), "ap_partitioner") {
  /** Index or entity this Partitioner belongs to*/
  def indexname = column[String]("index")

  /** How many partitions are to be used*/
  def noPartitions = column[Int]("noPartitions")

  /** Metadata about this partitioner*/
  def meta = column[Array[Byte]]("meta")

  def partitioner = column[Array[Byte]]("partitioner")

  def index = foreignKey("partitioner_index_fk", indexname, TableQuery[IndexCatalog])(_.indexname, onUpdate=ForeignKeyAction.Cascade, onDelete=ForeignKeyAction.Cascade)

  override def * = (indexname, noPartitions, meta, partitioner)
}