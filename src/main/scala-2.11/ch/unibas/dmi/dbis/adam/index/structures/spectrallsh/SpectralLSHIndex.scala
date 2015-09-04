package ch.unibas.dmi.dbis.adam.index.structures.spectrallsh

import breeze.linalg.{DenseMatrix, DenseVector}
import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString
import ch.unibas.dmi.dbis.adam.data.{IndexMeta, IndexMetaBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class SpectralLSHIndex(val indexname: IndexName, val tablename: TableName, val indexdata: DataFrame, trainResult: TrainResult)
  extends Index {

  case class MovableDenseVector(d : WorkingVector) {
    def move(radius : Float) : WorkingVector = {
      val diff = DenseVector.fill(d.length)(radius - 2 * radius * Random.nextFloat)
      d + diff
    }
  }

  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def query(q: WorkingVector, options: Map[String, String]): Seq[TupleID] = {
    val k = options("k").toInt

    val radius = 0.1 //TODO: get this information during training

    val movableQuery = MovableDenseVector(q)
    val queries = List(q, movableQuery.move(radius), movableQuery.move(radius), movableQuery.move(radius), movableQuery.move(radius), movableQuery.move(radius))

    val queryHashes = queries.map{q => SpectralLSHUtils.hashFeature(q, trainResult)}

    //TODO take ordered
    val results = indexdata
      .map { tuple =>
      val bits = BitString.fromByteArray(tuple.getSeq[Byte](1).toArray)
      IndexTuple(tuple.getLong(0), bits)
    }
      .map(indexTuple => {

      val score : Int = queryHashes.map{query =>
        indexTuple.value.intersectionCount(query)
      }.sum

      (indexTuple.tid, score)
    }).sortBy { case (tid, score) => -score }

    results.map(_._1).take(k * 5)
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

}

object SpectralLSHIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: IndexMeta) : Index =  {
    val pca_rows = meta.get("pca_rows").toString.toInt
    val pca_cols = meta.get("pca_cols").toString.toInt
    val pca_array = meta.get[List[Double]]("pca").map(_.toFloat).toArray
    val pca = new DenseMatrix(pca_rows, pca_cols, pca_array)

    val min = new DenseVector(meta.get[List[Double]]("min").toArray)
    val max = new DenseVector(meta.get[List[Double]]("max").toArray)

    val modes_rows = meta.get("modes_rows").toString.toInt
    val modes_cols = meta.get("modes_cols").toString.toInt
    val modes_array = meta.get[List[Double]]("modes").map(_.toFloat).toArray
    val modes = new DenseMatrix(modes_rows, modes_cols, modes_array)

    val trainResult = TrainResult(pca, min, max, modes)

    new SpectralLSHIndex(indexname, tablename, data, trainResult)
  }
}
