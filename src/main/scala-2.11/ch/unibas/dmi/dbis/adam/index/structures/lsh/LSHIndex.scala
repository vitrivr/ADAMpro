package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.Hasher
import ch.unibas.dmi.dbis.adam.index.{Index, IndexMetaStorage, IndexMetaStorageBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.FutureAction
import org.apache.spark.sql.DataFrame


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class LSHIndex(val indexname : IndexName, val tablename : TableName, protected val indexdata: DataFrame, private val indexMetaData: LSHIndexMetaData)
  extends Index {

  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def scan(q: WorkingVector, options: Map[String, String]): FutureAction[Seq[TupleID]] = {
    import ch.unibas.dmi.dbis.adam.data.types.MovableFeature.conv_feature2MovableFeature
    val queries = List.fill(5)(q.move(0.1))

    val results = indexdata
      .map{ tuple =>
        IndexTuple(tuple.getLong(0), tuple.getAs[BitString[_]](1)) }
      .filter { indexTuple =>
      indexTuple.bits.getIndexes.zip(queries).exists({
        case (indexHash, acceptedValues) => acceptedValues.contains(indexHash)
      })
    }.map { indexTuple => indexTuple.tid }

    results.collectAsync()
  }

  /**
   *
   * @return
   */
  override private[index] def prepareMeta(metaBuilder : IndexMetaStorageBuilder) : Unit = {
    metaBuilder.put("radius", indexMetaData.radius)
    metaBuilder.put("hashtables", indexMetaData.hashTables)
  }

}

object LSHIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: IndexMetaStorage) : Index =  {
    val hashTables : Seq[Hasher] = meta.get("hashtables")
    val radius : Float = meta.get("radius")

    val indexMetaData = LSHIndexMetaData(hashTables, radius)

    new LSHIndex(indexname, tablename, data, indexMetaData)
  }
}