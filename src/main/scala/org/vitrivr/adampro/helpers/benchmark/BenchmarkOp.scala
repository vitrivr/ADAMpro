package org.vitrivr.adampro.helpers.benchmark

import org.vitrivr.adampro.helpers.benchmark.ScanWeightCatalogOperator.ScanWeightedIndex
import org.vitrivr.adampro.main.AdamContext

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
object BenchmarkOp {


  /**
    *
    * @param ic
    * @param qc
    */
  def benchmarkAndUpdateWeight(ic: IndexCollection, qc: QueryCollection)(implicit ac: AdamContext): Try[Void] = {
    try {
      val benchmarker = new Benchmarker(ic.getIndexes, qc.getQueries)
      val scores = benchmarker.getScores()

      scores.foreach {
        score =>
          score._1.setScanWeight(score._2)
      }

      //TODO: improve coding
      if (ic.isInstanceOf[NewIndexCollection]) {
        //drop the too many newly created indexes
        val indexScores = scores.toSeq.filter(_._1.isInstanceOf[ScanWeightedIndex])
          .map(_._1.asInstanceOf[ScanWeightedIndex])
          .sortBy(_.getScanWeight())

        indexScores.dropRight(math.min(indexScores.size, 3)).foreach { index =>
          index.index.drop()
        }
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}
