package org.vitrivr.adampro.data.index.structures.mi

import org.vitrivr.adampro.data.index.IndexingTaskTuple

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[mi]
case class MIIndexMetaData(ki : Int, ks : Int, refs : Seq[IndexingTaskTuple]) extends Serializable