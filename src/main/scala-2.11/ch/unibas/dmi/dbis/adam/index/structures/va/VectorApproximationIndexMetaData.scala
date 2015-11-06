package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.index.structures.va.VectorApproximationIndex.Marks
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[va]
case class VectorApproximationIndexMetaData(marks : Marks, signatureGenerator : SignatureGenerator, distance : MinkowskiDistance) extends Serializable