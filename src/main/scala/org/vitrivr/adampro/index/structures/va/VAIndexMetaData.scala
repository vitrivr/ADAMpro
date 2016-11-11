package org.vitrivr.adampro.index.structures.va

import org.vitrivr.adampro.index.structures.va.VAIndex.Marks
import org.vitrivr.adampro.index.structures.va.signature.SignatureGenerator

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SerialVersionUID(100L)
private[va] case class VAIndexMetaData(marks : Marks, signatureGenerator : SignatureGenerator) extends Serializable