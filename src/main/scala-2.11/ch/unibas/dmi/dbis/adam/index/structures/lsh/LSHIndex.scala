package ch.unibas.dmi.dbis.adam.index.structures.lsh

import java.io._
import java.util.Base64

import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.MovableFeature
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.Hasher
import ch.unibas.dmi.dbis.adam.index.structures.spectrallsh.results.SpectralLSHResultHandler
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
class LSHIndex(val indexname: IndexName, val tablename: TableName, protected val indexdata: DataFrame, private val indexMetaData: LSHIndexMetaData)
  extends Index {

  /**
   *
   * @return
   */
  override protected lazy val indextuples: RDD[IndexTuple] = {
    indexdata
      .map { tuple =>
      IndexTuple(tuple.getLong(0), tuple.getAs[BitString[_]](1))
    }
  }

  /**
   *
   * @param q
   * @param options
   * @return
   */
  override def scan(q: WorkingVector, options: Map[String, String]): HashSet[Int] = {
    val k = options("k").toInt
    val numOfQueries = options.getOrElse("numOfQ", "3").toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = LSHUtils.hashFeature(q, indexMetaData)
    val queries = (List.fill(numOfQueries)(LSHUtils.hashFeature(q.move(indexMetaData.radius), indexMetaData)) ::: List(originalQuery)).par

    val it = indextuples
      .mapPartitions(tuplesIt => {
      val localRh = new SpectralLSHResultHandler(k)

      while (tuplesIt.hasNext) {
        val tuple = tuplesIt.next()

        var i = 0
        var score = 0
        while (i < queries.length) {
          val query = queries(i)
          score += tuple.bits.intersectionCount(query)
          i += 1
        }

        localRh.offerIndexTuple(tuple, score)
      }

      localRh.iterator
    }).collect().iterator


    val globalResultHandler = new SpectralLSHResultHandler(k)
    globalResultHandler.offerResultElement(it)
    val ids = globalResultHandler.results.map(x => x.tid).toList

    HashSet(ids.map(_.toInt): _*)
  }

  /**
   *
   * @return
   */
  override private[index] def prepareMeta(metaBuilder: IndexMetaStorageBuilder): Unit = {
    metaBuilder.put("radius", indexMetaData.radius)


    //TODO: change this!!!
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    oos.writeObject(indexMetaData.hashTables)
    oos.close()

    metaBuilder.put("hashtables", Base64.getEncoder().encodeToString(baos.toByteArray()))
  }

  /**
   *
   */
  override val indextypename: IndexTypeName = "lsh"
}

object LSHIndex {
  def apply(indexname: IndexName, tablename: TableName, data: DataFrame, meta: IndexMetaStorage): Index = {

    //TODO: change this!!!
    val hashTablesString  = meta.get("hashtables").toString
    val ois = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(hashTablesString)))
    val hashTables = ois.readObject().asInstanceOf[Seq[Hasher]]
    ois.close()

    val radius = meta.get("radius").toString.toFloat

    val indexMetaData = LSHIndexMetaData(hashTables, radius)

    new LSHIndex(indexname, tablename, data, indexMetaData)
  }
}