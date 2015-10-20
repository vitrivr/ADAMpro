package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.Feature.{VectorBase, WorkingVector, _}
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.SpectralLSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.{NewVectorApproximationIndexer, VectorApproximationIndexer}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexerTuple}
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexOp {
  /**
   *
   * @param tablename
   * @param indextype
   * @param properties
   */
  def apply(tablename : TableName, indextype : String, properties : Map[String, String]): Unit = {
    val indextypename = IndexStructures.withName(indextype)
    apply(tablename, indextypename, properties)
  }

  /**
   *
   * @param tablename
   * @param indextypename
   * @param properties
   */
  def apply(tablename : TableName, indextypename : IndexTypeName, properties : Map[String, String]): Unit = {
    val table = Table.retrieveTable(tablename)

    //TODO: change this so that indices do not have to look at data before creation
    val data: RDD[IndexerTuple[WorkingVector]] = table.rows.map { x => IndexerTuple(x.getLong(0), x.getSeq[VectorBase](1) : WorkingVector) }

    val generator : IndexGenerator = indextypename match {
      case IndexStructures.ECP => ECPIndexer(properties, data)
      case IndexStructures.LSH => LSHIndexer(properties, data)
      case IndexStructures.SH => SpectralLSHIndexer(properties, data)
      case IndexStructures.VAF => VectorApproximationIndexer(properties, data)
      case IndexStructures.VAV => NewVectorApproximationIndexer(properties, data)
    }

    Index.createIndex(table, generator)
  }
}
