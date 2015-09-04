package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.data.{IndexMeta, IndexMetaBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.IndexName
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndexer.{Bounds, Marks}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.results.{BoundableResultHandler, ResultHandler}
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.{FixedSignatureGenerator, SignatureGenerator}
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class VectorApproximationIndex(val indexname : IndexName, val tablename : TableName, val indexdata: DataFrame, marks : Marks, signatureGenerator : SignatureGenerator)
  extends Index with Serializable {

  /**
   *
   */
  def query(q: WorkingVector, options: Map[String, String]): Seq[TupleID] = {
    val k = options("k").toInt
    val norm = options("norm").toInt

    val lbounds: Bounds = lowerBounds(q, marks, new NormBasedDistanceFunction(norm))
    val ubounds: Bounds = upperBounds(q, marks, new NormBasedDistanceFunction(norm))

    val localResultHandlers = indexdata
      .map{ tuple =>
      val bits = BitString.fromByteArray(tuple.getSeq[Byte](1).toArray)
      IndexTuple(tuple.getLong(0), bits) }
      .mapPartitions(indexTuplesIt => {
      val rh = new BoundableResultHandler(k, lbounds, ubounds, signatureGenerator)
      rh.offerIndexTuple(indexTuplesIt)
      rh.iterator
    }).collect()

    val globalResultHandler = new ResultHandler(k)
    globalResultHandler.offerResultElement(localResultHandlers.iterator)

    globalResultHandler.results.map(_.indexTuple.tid)
  }

  /**
   *
   */
  def lowerBounds(q: WorkingVector, marks: => Marks, distance: NormBasedDistanceFunction): Bounds = {
    val bounds = marks.zipWithIndex.map {
      case (dimMarks, i) =>
        val fvi = q(i)
        dimMarks.iterator.sliding(2).withPartial(false).map { //TODO: valuesIterator?
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
   */
  def upperBounds(q: WorkingVector, marks: => Marks, distance: NormBasedDistanceFunction): Bounds = {
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


  override def getMeta: IndexMeta = {
    val metadataBuilder = new IndexMetaBuilder()
    metadataBuilder.put("marks", marks)
    metadataBuilder.put("signatureGenerator", signatureGenerator)
    metadataBuilder.build()
  }

}

object VectorApproximationIndex {
  def apply(indexname : IndexName, tablename : TableName, data: DataFrame, meta : IndexMeta ) : Index = {
    val marks : Marks = meta.get("marks").asInstanceOf[Seq[Seq[Double]]].map(_.map(_.toFloat))
    val signatureGenerator =  meta.get("signatureGenerator").asInstanceOf[FixedSignatureGenerator]

    new VectorApproximationIndex(indexname, tablename, data,
      marks, signatureGenerator
     )
  }
}