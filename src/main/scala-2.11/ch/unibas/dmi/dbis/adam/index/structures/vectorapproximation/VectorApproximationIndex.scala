package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex.{Bounds, Marks}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results.VectorApproximationResultHandler
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.FixedSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{Index, IndexMetaStorage, IndexMetaStorageBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ComplexFutureAction, FutureAction}
import scala.concurrent.ExecutionContext.Implicits.global

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
   */
  override def scan(q: WorkingVector, options: Map[String, String]): FutureAction[Seq[TupleID]] = {
    val k = options("k").toInt
    val norm = options("norm").toInt

    val lbounds: Bounds = lowerBounds(q, indexMetaData.marks, new NormBasedDistanceFunction(norm))
    val ubounds: Bounds = upperBounds(q, indexMetaData.marks, new NormBasedDistanceFunction(norm))

    val globalResultHandler = new VectorApproximationResultHandler(k)

    indexdata
      .map{ tuple =>
        val bits = BitString.fromByteArray(tuple.getSeq[Byte](1).toArray)
        val indexTuple : IndexTuple = IndexTuple(tuple.getLong(0), bits)
        indexTuple }
      .mapPartitions(tuplesIt => {
        val localRh = new VectorApproximationResultHandler(k, lbounds, ubounds, indexMetaData.signatureGenerator)
        tuplesIt.foreach { tuple =>
          localRh.offerResultElement(tuple)
        }
        localRh.iterator})
      .foreachAsync(x => globalResultHandler.offerResultElement(x))

    val action = new ComplexFutureAction[Seq[TupleID]]
    action.run({
      globalResultHandler.results.map(x => x.indexTuple.tid).toList
    })

    action
  }


  /**
   *
   * @param bounds
   * @param signature
   * @return
   */
  private def computeBounds(bounds: Bounds, signature: BitString[_]): Distance = {
    val cells = indexMetaData.signatureGenerator.toCells(signature)

    var sum : Float = 0
    cells.zipWithIndex.foreach { case(cell, index) =>
      sum += bounds(index)(cell)
    }

    sum
  }

  /**
   *
   * @param q
   * @param marks
   * @param distance
   * @return
   */
  private def lowerBounds(q: WorkingVector, marks: => Marks, distance: NormBasedDistanceFunction): Bounds = {
    val bounds = marks.zipWithIndex.map {
      case (dimMarks, i) =>
        val fvi = q(i)
        dimMarks.iterator.sliding(2).withPartial(false).map {
          dimMark =>
            if (fvi < dimMark(0)) {
              distance(dimMark(0), fvi)
            } else if (fvi > dimMark(1)) {
              distance(fvi, dimMark(1))
            } else {
              0.toFloat
            }
        }.toArray
    }.toArray

    bounds
  }

  /**
   *
   * @param q
   * @param marks
   * @param distance
   * @return
   */
  private def upperBounds(q: WorkingVector, marks: => Marks, distance: NormBasedDistanceFunction): Bounds = {
    val bounds = marks.zipWithIndex.map {
      case (dimMarks, i) =>
        val fvi = q(i)

        dimMarks.iterator.sliding(2).withPartial(false).map { //TODO: valuesIterator?
          dimMark =>
            if (fvi <= (dimMark(0) + dimMark(1)) / 2.toFloat) {
              distance(dimMark(1), fvi)
            } else {
              distance(fvi, dimMark(0))
            }
        }.toArray
    }.toArray

    bounds
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
    val signatureGenerator =  meta.get("signatureGenerator").asInstanceOf[FixedSignatureGenerator]

    val indexMetaData = VectorApproximationIndexMetaData(marks, signatureGenerator)

    new VectorApproximationIndex(indexname, tablename, data, indexMetaData)
  }
}