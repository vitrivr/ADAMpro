package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.datatypes.feature.MovableFeature
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.lsh.results.LSHResultHandler
import ch.unibas.dmi.dbis.adam.index.{BitStringIndexTuple, Index}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import org.apache.spark.TaskContext
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.HashSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class LSHIndex(val indexname: IndexName, val entityname: EntityName, protected val df: DataFrame, private[index] val metadata: LSHIndexMetaData)
  extends Index[BitStringIndexTuple] {

  override val indextype: IndexTypeName = IndexStructures.LSH
  override val confidence = 0.toFloat

  override def scan(data : DataFrame, q : FeatureVector, options : Map[String, Any], k : Int): HashSet[TupleID] = {
    val numOfQueries = options.getOrElse("numOfQ", "3").asInstanceOf[String].toInt

    import MovableFeature.conv_feature2MovableFeature
    val originalQuery = LSHUtils.hashFeature(q, metadata)
    val queries = (List.fill(numOfQueries)(LSHUtils.hashFeature(q.move(metadata.radius), metadata)) ::: List(originalQuery)).par

    val rdd = df.map(r => r : BitStringIndexTuple)

    val results = SparkStartup.sc.runJob(rdd, (context : TaskContext, tuplesIt : Iterator[BitStringIndexTuple]) => {
      val localRh = new LSHResultHandler(k)
      while (tuplesIt.hasNext) {
        val tuple = tuplesIt.next()

        var i = 0
        var score = 0
        while (i < queries.length) {
          val query = queries(i)
          score += tuple.value.intersectionCount(query)
          i += 1
        }

        localRh.offerIndexTuple(tuple, score)
      }

      localRh.results.toSeq
    }).flatten

    val globalResultHandler = new LSHResultHandler(k)
    globalResultHandler.offerResultElement(results.iterator)
    val ids = globalResultHandler.results.map(x => x.tid).toList

    HashSet(ids : _*)
  }
}

object LSHIndex {
  def apply(indexname: IndexName, entityname: EntityName, data: DataFrame, meta: Any): LSHIndex = {
    val indexMetaData = meta.asInstanceOf[LSHIndexMetaData]
    new LSHIndex(indexname, entityname, data, indexMetaData)
  }
}