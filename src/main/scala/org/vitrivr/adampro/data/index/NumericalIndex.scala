package org.vitrivr.adampro.data.index

import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.data.datatypes.vector.{ADAMNumericalVector, ADAMVector}
import org.vitrivr.adampro.data.datatypes.vector.Vector.MathVector
import org.vitrivr.adampro.data.index.Index.IndexName
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.DistanceFunction
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.utils.exception.GeneralAdamException

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2018
  */
abstract class NumericalIndex (override val indexname: IndexName)(@transient override implicit val ac: SharedComponentContext) extends Index(indexname)(ac) with Serializable with Logging {

  protected def scan(data: DataFrame, q: ADAMVector[_], distance: DistanceFunction, options: Map[String, String], k: Int)(tracker: QueryTracker): DataFrame = {
    if(q.isInstanceOf[ADAMNumericalVector]){
      scan(data, q.asInstanceOf[ADAMNumericalVector].values, distance, options, k)(tracker)
    } else {
      throw new GeneralAdamException("Numerical index accepts only numerical vector")
    }
  }



  /**
    * Scans the index.
    *
    * @param data     rdd to scan
    * @param q        query vector
    * @param distance distance funciton
    * @param options  options to be passed to the index reader
    * @param k        number of elements to retrieve (of the k nearest neighbor search), possibly more than k elements are returned
    * @return a set of candidate tuple ids, possibly together with a tentative score (the number of tuples will be greater than k)
    */
  protected def scan(data: DataFrame, q: MathVector, distance: DistanceFunction, options: Map[String, String], k: Int)(tracker: QueryTracker): DataFrame


}
