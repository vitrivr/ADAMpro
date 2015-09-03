package ch.unibas.dmi.dbis.adam.cli.operations

import ch.unibas.dmi.dbis.adam.data.IndexTuple
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.SpectralLSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.index.IndexGenerator

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
    val data = table.data.map { x => IndexTuple(x.getInt(0), x.getSeq[VectorBase](1) : WorkingVector) }

    val generator : IndexGenerator = indextype match {
      case "lsh" =>  LSHIndexer(properties, data)
      case "slsh" =>  SpectralLSHIndexer(properties, data)
      case "va" =>  VectorApproximationIndexer(properties, data)
    }


    Index.createIndex(table, generator)
  }
}
