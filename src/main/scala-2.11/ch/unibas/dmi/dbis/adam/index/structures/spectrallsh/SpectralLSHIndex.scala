package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import java.util.BitSet

import breeze.linalg.{*, DenseVector, DenseMatrix}
import ch.unibas.dmi.dbis.adam.data.{IndexMetaBuilder, IndexMeta, IndexTuple}
import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.DataFrame

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SpectralLSHIndex(val indexname: IndexName, val tablename: TableName, val indexdata: DataFrame, trainResult: TrainResult)
  extends Index {


  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def query(q: WorkingVector, options: Map[String, String]): Seq[TupleID] = {
    val queryHash = BitSet.valueOf(hashFeature(q, trainResult))

    val results = indexdata
      .map { tuple => IndexTuple(tuple.getLong(0), BitSet.valueOf(tuple.getAs[Array[Byte]](1))) }
      .map(indexTuple => {
      indexTuple.value.or(queryHash)
      (indexTuple.tid, indexTuple.value.cardinality())
    }).sortBy { case (tid, score) => score }.map(_._1)

    results.collect
  }


  override def getMeta: IndexMeta = {
    val metadataBuilder = new IndexMetaBuilder()
    metadataBuilder.put("pca", trainResult.pca.toDenseMatrix.toArray)
    metadataBuilder.put("pca_cols", trainResult.pca.cols)
    metadataBuilder.put("pca_rows", trainResult.pca.rows)
    metadataBuilder.put("min", trainResult.min.toArray)
    metadataBuilder.put("max", trainResult.max.toArray)
    metadataBuilder.put("modes", trainResult.modes.toDenseMatrix.toArray)
    metadataBuilder.put("modes_cols", trainResult.modes.cols)
    metadataBuilder.put("modes_rows", trainResult.modes.rows)
    metadataBuilder.build()
  }

  /**
   *
   * @param f
   * @param trainResult
   * @return
   */
  @inline private def hashFeature(f : WorkingVector, trainResult : TrainResult) : Array[Byte] = {
    val fMat = f.toDenseMatrix
    val pca = trainResult.pca.toDenseMatrix

    val v = fMat.*(pca).asInstanceOf[DenseMatrix[Float]].toDenseVector - trainResult.min.toDenseVector

    val res = {
      val omegai : DenseMatrix[VectorBase] = trainResult.omegas(*, ::) :* v
      omegai :+= toVectorBase(Math.PI / 2.0)
      val ys = omegai.map(x => math.sin(x))
      val yi = ys(*, ::).map(_.toArray.product).toDenseVector

      val res = yi.findAll(x => x > 0)
      res.toArray
    }

    BitSet.valueOf(res.map(_.toLong)).toByteArray
  }
}

object SpectralLSHIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: IndexMeta) : Index =  {
    val pca_rows = meta.get("pca_rows")
    val pca_cols = meta.get("pca_cols")
    val pca_array = meta.get("pca").asInstanceOf[Array[VectorBase]]
    val pca = new DenseMatrix(pca_rows, pca_cols, pca_array)

    val min = new DenseVector(meta.get("min").asInstanceOf[Array[VectorBase]])
    val max = new DenseVector(meta.get("max").asInstanceOf[Array[VectorBase]])

    val modes_rows = meta.get("modes_rows")
    val modes_cols = meta.get("modes_cols")
    val modes_array = meta.get("modes").asInstanceOf[Array[VectorBase]]
    val modes = new DenseMatrix(modes_rows, modes_cols, modes_array)

    val trainResult = TrainResult(pca, min, max, modes)

    new SpectralLSHIndex(indexname, tablename, data, trainResult)
  }
}
