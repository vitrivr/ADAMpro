package ch.unibas.dmi.dbis.adam.index.structures.lsh

import breeze.linalg.DenseVector
import ch.unibas.dmi.dbis.adam.data.Tuple._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.data.types.bitString.BitString.BitStringType
import ch.unibas.dmi.dbis.adam.data.{IndexMeta, IndexMetaBuilder, IndexTuple}
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.Hasher
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.DataFrame

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class LSHIndex(val indexname : IndexName, val tablename : TableName, val indexdata: DataFrame, hashTables : Seq[Hasher], radius : Float)
  extends Index {

  /**
   *
   * @param q
   * @param options
   * @return
   */
  def query(q : WorkingVector, options : Map[String, String]) : Seq[TupleID] = {
    val extendedQuery = getExtendedQuery(q, radius)

    val results = indexdata
      .map{ tuple =>
      IndexTuple(tuple.getLong(0), tuple.getAs[BitStringType](1)) }
      .filter { indexTuple =>
      indexTuple.value.getIndexes.zip(extendedQuery).exists({
        case (indexHash, acceptedValues) => acceptedValues.contains(indexHash)
      })
    }.map { indexTuple => indexTuple.tid }

    results.collect
  }

  /**
   *
   * @param q
   * @param radius
   * @return
   */
  private def getExtendedQuery(q : WorkingVector, radius : Float) = {
    val queries = (1 to 10).map(x => q.move(radius)).:+(q)
    val hashes = queries.map(qVector => hashTables.map(ht => ht(qVector)))

    val hashesPerDim = (0 until q.length).map { i =>
      hashes.map { hash => hash(i) }.distinct
    }

    hashesPerDim
  }

  /**
   *
   * @param d
   */
  //TODO use moveable vector
  implicit class MovableDenseVector(d : WorkingVector) {
    def move(radius : Float) : WorkingVector = {
      val diff = DenseVector.fill(d.length)(radius - 2 * radius * Random.nextFloat)
      d + diff
    }
  }

  override def getMeta: IndexMeta = {
    val metaBuilder = new IndexMetaBuilder()
    metaBuilder.put("radius", radius)
    metaBuilder.put("hashtables", hashTables)
    metaBuilder.build()
  }

}

object LSHIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: IndexMeta) : Index =  {
    val hashTables : Seq[Hasher] = meta.get("hashtables")
    val radius : Float = meta.get("radius")

    new LSHIndex(indexname, tablename, data, hashTables, radius)
  }
}