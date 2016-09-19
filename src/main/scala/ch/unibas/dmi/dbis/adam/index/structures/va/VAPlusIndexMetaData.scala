package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex._
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.SignatureGenerator
import org.apache.spark.mllib.feature.PCAModel

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[va] class VAPlusIndexMetaData(override val marks: Marks, override val signatureGenerator: SignatureGenerator, val pca: PCAModel, val approximate : Boolean) extends VAIndexMetaData(marks, signatureGenerator) with Serializable {}
