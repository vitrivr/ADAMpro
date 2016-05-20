package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex.Marks
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.SignatureGenerator
import ch.unibas.dmi.dbis.adam.query.distance.MinkowskiDistance

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SerialVersionUID(100L)
private[va] case class VAIndexMetaData(marks : Marks, signatureGenerator : SignatureGenerator, distance : MinkowskiDistance) extends Serializable