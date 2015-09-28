package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.MovableFeature
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.Hasher
import ch.unibas.dmi.dbis.adam.index.{Index, IndexMetaStorage, IndexMetaStorageBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet


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
   * @return
   */
  override protected lazy val indextuples : RDD[IndexTuple] = {
    indexdata
      .map{ tuple =>
      IndexTuple(tuple.getLong(0), tuple.getAs[BitString[_]](1)) }
  }

  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def scan(q: WorkingVector, options: Map[String, String]): HashSet[Int] = {
    import MovableFeature.conv_feature2MovableFeature
    val queries = List.fill(5)(q.move(0.1))

    val ids =  indextuples
      .filter { indexTuple =>
      indexTuple.bits.getIndexes.zip(queries).exists({
        case (indexHash, acceptedValues) => acceptedValues.contains(indexHash)
      })
    }.map { indexTuple => indexTuple.tid }.collect

    HashSet(ids.map(_.toInt):_*)
  }

  /**
   *
   * @return
   */
  override private[index] def prepareMeta(metaBuilder : IndexMetaStorageBuilder) : Unit = {
    metaBuilder.put("radius", indexMetaData.radius)
    metaBuilder.put("hashtables", indexMetaData.hashTables)
  }

  /**
   *
   */
  override val indextypename: IndexTypeName = "lsh"
}

object LSHIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: IndexMetaStorage) : Index =  {
    val hashTables : Seq[Hasher] = meta.get("hashtables")
    val radius : Float = meta.get("radius")

    val indexMetaData = LSHIndexMetaData(hashTables, radius)

    new LSHIndex(indexname, tablename, data, indexMetaData)
  }
}