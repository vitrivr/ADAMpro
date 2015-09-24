package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex.{Bounds, Marks}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results.VectorApproximationResultHandler
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{Index, IndexMetaStorage, IndexMetaStorageBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
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
class VectorApproximationIndex(val indexname : IndexName, val tablename : TableName, protected val indexdata: DataFrame, private val indexMetaData: VectorApproximationIndexMetaData)
  extends Index with Serializable {

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
   */
  override def scan(q: WorkingVector, options: Map[String, String]): HashSet[Int] = {
    val k = options("k").toInt
    val norm = options("norm").toInt
    
    val (lbounds, ubounds) = computeBounds(q, indexMetaData.marks, new NormBasedDistanceFunction(norm))

    val it = indextuples
      .mapPartitions(tuplesIt => {
      val localRh = new VectorApproximationResultHandler(k, lbounds, ubounds, indexMetaData.signatureGenerator)
      localRh.offerIndexTuple(tuplesIt)
      localRh.iterator}).collect().iterator

    val globalResultHandler = new VectorApproximationResultHandler(k)
    globalResultHandler.offerResultElement(it)
    val ids = globalResultHandler.results.map(x => x.tid).toList

    HashSet(ids.map(_.toInt):_*)
  }

  /**
   *
   * @param q
   * @param marks
   * @param distance
   * @return
   */
  private[this] def computeBounds(q: WorkingVector, marks: => Marks, @inline distance: NormBasedDistanceFunction): (Bounds, Bounds) = {
    val lbounds, ubounds = Array.tabulate(marks.length)(i => Array.ofDim[Distance](marks(i).length - 1))

     var i = 0
     while(i < marks.length) {
        val dimMarks = marks(i)
        val fvi = q(i)

        var j = 0
        val it = dimMarks.iterator.sliding(2).withPartial(false)

        while(it.hasNext){
          val dimMark = it.next()

          lazy val d0fv1 = distance(dimMark(0), fvi)
          lazy val d1fv1 = distance(dimMark(1), fvi)

          if (fvi < dimMark(0)) {
            lbounds(i)(j) = d0fv1
          } else if (fvi > dimMark(1)) {
            lbounds(i)(j) = d1fv1
          }

          if (fvi <= (dimMark(0) + dimMark(1)) / 2.toFloat) {
            ubounds(i)(j) = d1fv1
          } else {
            ubounds(i)(j) = d0fv1
          }

          j += 1
        }

         i += 1
    }

    (lbounds, ubounds)
  }


  /**
   *
   * @param metaBuilder
   */
  override private[index] def prepareMeta(metaBuilder : IndexMetaStorageBuilder) : Unit = {
    metaBuilder.put("marks", indexMetaData.marks)
    metaBuilder.put("signatureGenerator", indexMetaData.signatureGenerator)
  }

}

object VectorApproximationIndex {
  type Marks = Seq[Seq[VectorBase]]
  type Bounds = Array[Array[Distance]]

  def apply(indexname : IndexName, tablename : TableName, data: DataFrame, meta : IndexMetaStorage ) : Index = {
    val marks : Marks = meta.get("marks").asInstanceOf[Seq[Seq[Double]]].map(_.map(_.toFloat))

    val signatureGenerator =  meta.get("signatureGenerator").asInstanceOf[SignatureGenerator]

    val indexMetaData = VectorApproximationIndexMetaData(marks, signatureGenerator)

    new VectorApproximationIndex(indexname, tablename, data, indexMetaData)
  }
}