package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.{Bounds, Marks}
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.{FixedSignatureGenerator, VariableSignatureGenerator}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.distance.Distance._
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class VAIndex(val indexname: IndexName, val entityname: EntityName, dataframe : DataFrame, private[index] val metadata: VAIndexMetaData)(@transient implicit val ac : AdamContext)
  extends Index(dataframe) with Serializable {

  override val indextypename: IndexTypeName = metadata.signatureGenerator match {
    case fsg: FixedSignatureGenerator => IndexTypes.VAFINDEX
    case vsg: VariableSignatureGenerator => IndexTypes.VAVINDEX
  }

  override val lossy: Boolean = false
  override val confidence = 1.toFloat

  override def scan(data: DataFrame, q: FeatureVector, distance: DistanceFunction, options: Map[String, Any], k: Int): Set[Result] = {
    log.debug("scanning VA-File index " + indexname)

    val bounds = computeBounds(q, metadata.marks, distance.asInstanceOf[MinkowskiDistance])
    val lbounds = ac.sc.broadcast(bounds._1)
    val ubounds = ac.sc.broadcast(bounds._2)

    import org.apache.spark.sql.functions.udf
    val distUDF = (bounds : Broadcast[Bounds]) => udf((c: BitString[_]) => {
      val cells = metadata.signatureGenerator.toCells(c)

      var bound : Float = 0

      var idx = 0
      while (idx < cells.length) {
        bound += bounds.value(idx)(cells(idx))
        idx += 1
      }

      bound
    })

    val results = data
      .withColumn("lbound", distUDF(lbounds)(df(FieldNames.featureIndexColumnName)))
      .withColumn("ubound", distUDF(ubounds)(df(FieldNames.featureIndexColumnName)))
        .mapPartitions(p => {
          val localRh = new VAResultHandler(k)

          while(p.hasNext){
            val current = p.next()
            localRh.offer(current)
          }

          localRh.results.iterator
        }).collect()

    val globalResultHandler = new VAResultHandler(k)
    results.sortBy(_.upper).foreach(result => globalResultHandler.offer(result))

    val ids = globalResultHandler.results

    log.debug("VA-File returning " + ids.length + " tuples")

    ids.map(result => Result(result.lower, result.tid)).toSet
  }

  override def isQueryConform(nnq: NearestNeighbourQuery): Boolean = {
    if (nnq.distance.isInstanceOf[MinkowskiDistance]) {
      return true
    }

    false
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
    while (i < marks.length) {
      val dimMarks = marks(i)
      val fvi = q(i)

      var j = 0
      val it = dimMarks.iterator.sliding(2).withPartial(false)

      while (it.hasNext) {
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

  def apply(indexname: IndexName, entityname: EntityName, data: DataFrame, meta: Any)(implicit ac : AdamContext): VAIndex = {
    val indexMetaData = meta.asInstanceOf[VAIndexMetaData]
    new VAIndex(indexname, entityname, data, indexMetaData)
  }
}