package org.vitrivr.adampro.data.index.structures.va

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.bitstring.BitString
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.data.index.Index
import org.vitrivr.adampro.data.index.Index.{IndexName, IndexTypeName}
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.data.index.structures.va.VAIndex.{Bounds, Marks}
import org.vitrivr.adampro.data.index.structures.va.signature.{FixedSignatureGenerator, VariableSignatureGenerator}
import org.vitrivr.adampro.query.distance.Distance._
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}
import org.vitrivr.adampro.query.query.RankingQuery
import org.apache.spark.sql.functions._
import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.tracker.QueryTracker

/**
  * adamtwo
  *
  * Ivan Giangreco
  * August 2015
  */
class VAIndex(override val indexname: IndexName)(@transient override implicit val ac: SharedComponentContext)
  extends Index(indexname)(ac) {

  val meta = metadata.get.asInstanceOf[VAIndexMetaData]

  override lazy val indextypename: IndexTypeName = meta.signatureGenerator match {
    case fsg: FixedSignatureGenerator => IndexTypes.VAFINDEX
    case vsg: VariableSignatureGenerator => IndexTypes.VAVINDEX
  }

  override lazy val lossy: Boolean = false
  override lazy val confidence = 1.toFloat
  override lazy val score : Float = if(indextypename.equals(IndexTypes.VAFINDEX)){
    0.9.toFloat //slightly less weight if fixed variable
  } else {
    1.toFloat
  }

  /**
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  override def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker : QueryTracker): DataFrame = {
    log.debug("scanning VA-File index " + indexname)

    val signatureGeneratorBc = ac.sc.broadcast(meta.signatureGenerator)
    tracker.addBroadcast(signatureGeneratorBc)

    val bounds = computeBounds(q, meta.marks, distance.asInstanceOf[MinkowskiDistance])
    val lboundsBc = ac.sc.broadcast(bounds._1)
    tracker.addBroadcast(lboundsBc)
    val uboundsBc = ac.sc.broadcast(bounds._2)
    tracker.addBroadcast(uboundsBc)

    //compute the cells
    val cellsUDF = udf((c: Array[Byte]) => {
      signatureGeneratorBc.value.toCells(BitString.fromByteArray(c))
    })

    //compute the approximate distance given the cells
    val distUDF = (boundsBc: Broadcast[Bounds]) => udf((cells: Seq[Int]) => {
      var bound : Distance = 0

      var idx = 0
      while (idx < cells.length) {
        bound += boundsBc.value(idx)(cells(idx))
        idx += 1
      }

      bound
    })

    import ac.spark.implicits._

    val tmp = data
      .withColumn("ap_cells", cellsUDF(data(AttributeNames.featureIndexColumnName)))
      .withColumn("ap_lbound", distUDF(lboundsBc)(col("ap_cells")))
      .withColumn("ap_ubound", distUDF(uboundsBc)(col("ap_cells"))) //note that this is computed lazy!

    val pk = this.pk.name.toString

    //local filtering
    val localRes = tmp
      .mapPartitions(pIt => {
        //in here  we compute for each partition the k nearest neighbours and collect the results
        val localRh = new VAResultHandler(k)

        while (pIt.hasNext) {
          val current = pIt.next()
          localRh.offer(current, pk)
        }

        localRh.results.sortBy(x => -x.ap_upper).iterator
      })

    // global refinement
    val globalRh = new VAResultHandler(k)
    val gIt = localRes.toLocalIterator()

    while (gIt.hasNext) {
      val current = gIt.next()
      globalRh.offer(current, pk)
    }

    val res = ac.sqlContext.createDataset(globalRh.results).toDF()

    res
  }

  override def isQueryConform(nnq: RankingQuery): Boolean = {
    if (nnq.distance.isInstanceOf[MinkowskiDistance]) {
      return true
    }

    false
  }

  /**
    * Computes the distances to all bounds.
    *
    * @param q        query vector
    * @param marks    marks
    * @param distance distance function
    * @return
    */
  private[this] def computeBounds(q: MathVector, marks: => Marks, @inline distance: MinkowskiDistance): (Bounds, Bounds) = {
    val lbounds, ubounds = Array.tabulate(marks.length)(i => Array.ofDim[Distance](math.max(0, marks(i).length - 1)))

    var i = 0
    while (i < marks.length) {
      val dimMarks = marks(i)
      val fvi = q(i)

      var j = 0
      val it = dimMarks.iterator.sliding(2).withPartial(false)

      while (it.hasNext) {
        val dimMark = it.next()

        lazy val d0fv1 = distance.element(dimMark(0), fvi)
        lazy val d1fv1 = distance.element(dimMark(1), fvi)

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
}