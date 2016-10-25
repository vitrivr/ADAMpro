package ch.unibas.dmi.dbis.adam.index.structures.mi

import ch.unibas.dmi.dbis.adam.index.IndexingTaskTuple

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
private[mi]
case class MIIndexMetaData(ki : Int, ks : Int, refs : Seq[IndexingTaskTuple[_]]) extends Serializable