package ch.unibas.dmi.dbis.adam.index.structures.ecp

import ch.unibas.dmi.dbis.adam.datatypes.Feature.WorkingVector
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
class ECPIndexer(distance : NormBasedDistanceFunction) extends IndexGenerator with Serializable {
  override def indextypename: IndexTypeName = "ecp"

  /**
   *
   * @param indexname
   * @param tablename
   * @param data
   * @return
   */
  override def index(indexname: IndexName, tablename: TableName, data: RDD[IndexerTuple[WorkingVector]]): Index[_ <: IndexTuple] = {
    val n = data.countApprox(5000).getFinalValue().mean.toInt
    val leaders = data.takeSample(true, math.sqrt(n).toInt)

    val indexdata = data.map(datum => {
        val minTID = leaders.map({ l =>
          (l.tid, distance.apply(datum.value, l.value))
        }).minBy(_._2)._1

        LongIndexTuple(datum.tid, minTID)
      })

    import SparkStartup.sqlContext.implicits._
    new ECPIndex(indexname, tablename, indexdata.toDF, ECPIndexMetaData(leaders.toArray.toSeq, distance))
  }
}

object ECPIndexer {
  /**
   *
   * @param properties
   */
  def apply(properties : Map[String, String] = Map[String, String](), data: RDD[IndexerTuple[WorkingVector]]) : IndexGenerator = {
    new ECPIndexer(new NormBasedDistanceFunction(1))
  }
}