package ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation

import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.VectorApproximationIndex.Marks
import ch.unibas.dmi.dbis.adam.index.structures.vectorapproximation.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
private[vectorapproximation]
case class VectorApproximationIndexMetaData(marks : Marks, signatureGenerator : SignatureGenerator, distance : MinkowskiDistance) extends Serializable