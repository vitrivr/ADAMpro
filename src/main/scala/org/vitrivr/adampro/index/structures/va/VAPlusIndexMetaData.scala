package org.vitrivr.adampro.index.structures.va

import org.vitrivr.adampro.index.structures.va.VAIndex._
import org.vitrivr.adampro.index.structures.va.signature.SignatureGenerator
import org.apache.spark.mllib.feature.PCAModel

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[adampro] class VAPlusIndexMetaData(override val marks: Marks, override val signatureGenerator: SignatureGenerator, val pca: PCAModel, val approximate : Boolean) extends VAIndexMetaData(marks, signatureGenerator) with Serializable {}
