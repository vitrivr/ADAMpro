package ch.unibas.dmi.dbis.adam.index.structures.pq

import org.apache.spark.mllib.clustering.KMeansModel

import scala.collection.immutable.IndexedSeq

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
private[pq]
case class PQIndexMetaData(models : IndexedSeq[KMeansModel], nsq : Int) {}
