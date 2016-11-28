package org.vitrivr.adampro.helpers.benchmark

import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.index.Index
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
    * @param ic collection of indexes
    * @param qc collection of queries
    */
  def benchmarkAndUpdateWeight(ic: IndexCollection, qc: QueryCollection)(implicit ac: AdamContext): Try[Void] = {
    try {
      val benchmarker = new NaiveBenchmarker(ic.getIndexes, qc.getQueries)
      val scores = benchmarker.getScores()

      scores.foreach {
        score =>
          score._1.setScanWeight(score._2)
      }

      //TODO: improve coding
      /*if (ic.isInstanceOf[NewIndexCollection]) {
        //drop the too many newly created indexes
        val indexScores = scores.toSeq.filter(_._1.isInstanceOf[ScanWeightedIndex])
          .map(_._1.asInstanceOf[ScanWeightedIndex])
          .sortBy(_.getScanWeight())

        indexScores.dropRight(math.min(indexScores.size, 3)).foreach { index =>
          index.index.drop()
        }
      }*/

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    *
    * @param index
    * @return
    */
  def getScore(index : Index) : Float = {
    ScanWeightCatalogOperator(index)
  }

  /**
    *
    * @param entity
    * @param attribute
    * @return
    */
  def getScore(entity : Entity, attribute : String): Float ={
    ScanWeightCatalogOperator(entity, attribute)
  }
}
