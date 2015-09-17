package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.Feature.{VectorBase, WorkingVector}
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.SpectralLSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.{NewVectorApproximationIndexer, VectorApproximationIndexer}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexerTuple}
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName

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
   */
  def apply(tablename : TableName, indextype : IndexTypeName, properties : Map[String, String]): Unit = {
    val table = Table.retrieveTable(tablename)

    //TODO: change this so that indices do not have to look at data before creation
    val data = table.data.map { x => IndexerTuple(x.getLong(0), x.getSeq[VectorBase](1) : WorkingVector) }

    //TODO: replace by enum
    val generator : IndexGenerator = indextype match {
      case "lsh" =>  LSHIndexer(properties, data)
      case "slsh" =>  SpectralLSHIndexer(properties, data)
      case "va" =>  VectorApproximationIndexer(properties, data)
      case "nva" =>  NewVectorApproximationIndexer(properties, data)
    }


    Index.createIndex(table, generator)
  }
}
