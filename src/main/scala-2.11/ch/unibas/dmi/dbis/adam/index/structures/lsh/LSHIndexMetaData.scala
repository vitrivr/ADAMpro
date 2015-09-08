package ch.unibas.dmi.dbis.adam.index.structures.lsh

import ch.unibas.dmi.dbis.adam.index.structures.lsh.hashfunction.Hasher

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[lsh] case class LSHIndexMetaData(hashTables : Seq[Hasher], radius : Float) {

}
