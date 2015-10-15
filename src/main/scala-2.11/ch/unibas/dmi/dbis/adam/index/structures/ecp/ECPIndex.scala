package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.Tuple.TupleID
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet
import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
class ECPIndex(val indexname: IndexName, val tablename: TableName, protected val indexdata: DataFrame, protected val indexMetaData : ECPIndexMetaData) extends Index {
  override val indextypename: IndexTypeName = "ecp"


  /**
   *
   * @return
   */
  override protected lazy val indextuples: RDD[LongIndexTuple] = {
    indexdata
      .map { tuple =>
      LongIndexTuple(tuple.getLong(0), tuple.getLong(0))
    }
  }

  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def scan(q: WorkingVector, options: Map[String, String]): HashSet[TupleID] = {
    val k = options("k").toInt

    val centroids = indexMetaData.leaders.map(l => {
      (l.tid, indexMetaData.distance.apply(q, l.value))
    }).sortBy(_._2).map(_._1)

    var results = ListBuffer[TupleID]()
    var i = 0
    while(i < centroids.length && results.length < k){
      results ++= indextuples.filter(_.bits == centroids(i)).map(_.tid).collect()
      i += 1
    }

    HashSet(results.toList : _*)
  }


  /**
   *
   */
  override private[index] def getMetadata(): Serializable = {
    indexMetaData
  }

}

object ECPIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: Any): ECPIndex = {
    val indexMetaData = meta.asInstanceOf[ECPIndexMetaData]
    new ECPIndex(indexname, tablename, data, indexMetaData)
  }
}