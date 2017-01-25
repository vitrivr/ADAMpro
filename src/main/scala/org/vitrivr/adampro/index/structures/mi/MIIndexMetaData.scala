package org.vitrivr.adampro.index.structures.mi

import org.vitrivr.adampro.index.IndexingTaskTuple

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[mi]
case class MIIndexMetaData(ki : Int, ks : Int, refs : Seq[IndexingTaskTuple]) extends Serializable