package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.{Bounds, Marks}
import ch.unibas.dmi.dbis.adam.index.structures.va.results.VAResultHandler
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.{FixedSignatureGenerator, VariableSignatureGenerator}
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class VAIndex(val indexname : IndexName, val entityname : EntityName, protected val df: DataFrame, private[index] val metadata: VAIndexMetaData)
  extends Index[BitStringIndexTuple] with Serializable {

  override val indextype: IndexTypeName = metadata.signatureGenerator match {
    case fsg : FixedSignatureGenerator =>   IndexStructures.VAF
    case vsg : VariableSignatureGenerator => IndexStructures.VAV
  }
  override val confidence = 1.toFloat

  override def scan(data : DataFrame, q : FeatureVector, options : Map[String, Any], k : Int): HashSet[TupleID] = {
    val distance = metadata.distance
    val (lbounds, ubounds) = computeBounds(q, metadata.marks, distance)

    val rdd = df.map(r => r : BitStringIndexTuple)

    val results = SparkStartup.sc.runJob(rdd, (context : TaskContext, tuplesIt : Iterator[BitStringIndexTuple]) => {
      val localRh = new VAResultHandler(k, lbounds, ubounds, metadata.signatureGenerator)
      localRh.offerIndexTuple(tuplesIt)
      localRh.results.toSeq
    }).flatten

    val globalResultHandler = new VAResultHandler(k)
    globalResultHandler.offerResultElement(results.iterator)
    val ids = globalResultHandler.results.map(x => x.tid).toList

    HashSet(ids : _*)
  }

  /**
   *
   * @param q
   * @param marks
   * @param distance
   * @return
   */
  private[this] def computeBounds(q: FeatureVector, marks: => Marks, @inline distance: MinkowskiDistance): (Bounds, Bounds) = {
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
}


object VAIndex {
  type Marks = Seq[Seq[VectorBase]]
  type Bounds = Array[Array[Distance]]

  def apply(indexname : IndexName, entityname : EntityName, data: DataFrame, meta : Any) : VAIndex = {
    val indexMetaData = meta.asInstanceOf[VAIndexMetaData]
    new VAIndex(indexname, entityname, data, indexMetaData)
  }
}